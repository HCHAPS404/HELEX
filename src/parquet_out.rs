use std::fs::{self, File};
use std::io::{self, BufWriter};
use std::path::Path;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use parquet::basic::{
    Compression, ConvertedType, Encoding, LogicalType, Repetition, Type as PhysicalType,
};
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::Type;

use crate::sso::SmallBytes;

pub struct UserDailyRow {
    pub user_id: SmallBytes,
    pub day: i32,
    pub sessions: i64,
    pub events: i64,
    pub revenue_cents: i64,
    pub top_category: Option<u16>,
}

pub struct CampaignDailyRow {
    pub campaign_id: Vec<u8>,
    pub channel: Vec<u8>,
    pub day: i32,
    pub sessions: i64,
    pub purchasing_sessions: i64,
    pub revenue_cents: i64,
    pub assisted_sessions: i64,
}

fn build_writer_props() -> Arc<WriterProperties> {
    Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            .set_encoding(Encoding::PLAIN)
            .build(),
    )
}

pub fn user_daily_schema() -> Arc<Type> {
    let fields = vec![
        Arc::new(
            Type::primitive_type_builder("user_id", PhysicalType::BYTE_ARRAY)
                .with_repetition(Repetition::REQUIRED)
                .with_logical_type(Some(LogicalType::String))
                .with_converted_type(ConvertedType::UTF8)
                .build()
                .unwrap(),
        ),
        Arc::new(
            Type::primitive_type_builder("day", PhysicalType::INT32)
                .with_repetition(Repetition::REQUIRED)
                .with_logical_type(Some(LogicalType::Date))
                .with_converted_type(ConvertedType::DATE)
                .build()
                .unwrap(),
        ),
        Arc::new(
            Type::primitive_type_builder("sessions", PhysicalType::INT64)
                .with_repetition(Repetition::REQUIRED)
                .build()
                .unwrap(),
        ),
        Arc::new(
            Type::primitive_type_builder("events", PhysicalType::INT64)
                .with_repetition(Repetition::REQUIRED)
                .build()
                .unwrap(),
        ),
        Arc::new(
            Type::primitive_type_builder("revenue_cents", PhysicalType::INT64)
                .with_repetition(Repetition::REQUIRED)
                .build()
                .unwrap(),
        ),
        Arc::new(
            Type::primitive_type_builder("top_category", PhysicalType::BYTE_ARRAY)
                .with_repetition(Repetition::OPTIONAL)
                .with_logical_type(Some(LogicalType::String))
                .with_converted_type(ConvertedType::UTF8)
                .build()
                .unwrap(),
        ),
    ];
    Arc::new(
        Type::group_type_builder("user_daily")
            .with_fields(fields)
            .build()
            .unwrap(),
    )
}

pub fn campaign_daily_schema() -> Arc<Type> {
    let fields = vec![
        Arc::new(
            Type::primitive_type_builder("campaign_id", PhysicalType::BYTE_ARRAY)
                .with_repetition(Repetition::REQUIRED)
                .with_logical_type(Some(LogicalType::String))
                .with_converted_type(ConvertedType::UTF8)
                .build()
                .unwrap(),
        ),
        Arc::new(
            Type::primitive_type_builder("channel", PhysicalType::BYTE_ARRAY)
                .with_repetition(Repetition::REQUIRED)
                .with_logical_type(Some(LogicalType::String))
                .with_converted_type(ConvertedType::UTF8)
                .build()
                .unwrap(),
        ),
        Arc::new(
            Type::primitive_type_builder("day", PhysicalType::INT32)
                .with_repetition(Repetition::REQUIRED)
                .with_logical_type(Some(LogicalType::Date))
                .with_converted_type(ConvertedType::DATE)
                .build()
                .unwrap(),
        ),
        Arc::new(
            Type::primitive_type_builder("sessions", PhysicalType::INT64)
                .with_repetition(Repetition::REQUIRED)
                .build()
                .unwrap(),
        ),
        Arc::new(
            Type::primitive_type_builder("purchasing_sessions", PhysicalType::INT64)
                .with_repetition(Repetition::REQUIRED)
                .build()
                .unwrap(),
        ),
        Arc::new(
            Type::primitive_type_builder("revenue_cents", PhysicalType::INT64)
                .with_repetition(Repetition::REQUIRED)
                .build()
                .unwrap(),
        ),
        Arc::new(
            Type::primitive_type_builder("assisted_sessions", PhysicalType::INT64)
                .with_repetition(Repetition::REQUIRED)
                .build()
                .unwrap(),
        ),
    ];
    Arc::new(
        Type::group_type_builder("campaign_daily")
            .with_fields(fields)
            .build()
            .unwrap(),
    )
}

pub const BATCH_ROWS: usize = 65_536;

/// Incremental user_daily writer: one row group per `write_batch` call
/// (callers batch ~64k rows), so rows never accumulate in memory.
pub struct UserDailyWriter {
    writer: SerializedFileWriter<BufWriter<File>>,
    cat_bytes: Vec<Bytes>,
    user_ids: Vec<ByteArray>,
    days: Vec<i32>,
    sessions: Vec<i64>,
    events: Vec<i64>,
    revenues: Vec<i64>,
    cat_defs: Vec<i16>,
    cat_vals: Vec<ByteArray>,
}

fn perr(e: parquet::errors::ParquetError) -> io::Error {
    io::Error::other(e.to_string())
}

impl UserDailyWriter {
    pub fn create(path: &Path, categories: &[Vec<u8>]) -> io::Result<Self> {
        let file = BufWriter::with_capacity(1 << 20, File::create(path)?);
        let writer = SerializedFileWriter::new(file, user_daily_schema(), build_writer_props())
            .map_err(perr)?;
        Ok(Self {
            writer,
            cat_bytes: categories.iter().map(|c| Bytes::copy_from_slice(c)).collect(),
            user_ids: Vec::new(),
            days: Vec::new(),
            sessions: Vec::new(),
            events: Vec::new(),
            revenues: Vec::new(),
            cat_defs: Vec::new(),
            cat_vals: Vec::new(),
        })
    }

    pub fn write_batch(&mut self, chunk: &[UserDailyRow]) -> io::Result<()> {
        if chunk.is_empty() {
            return Ok(());
        }
        self.user_ids.clear();
        self.days.clear();
        self.sessions.clear();
        self.events.clear();
        self.revenues.clear();
        self.cat_defs.clear();
        self.cat_vals.clear();
        let id_bytes: usize = chunk.iter().map(|r| r.user_id.as_slice().len()).sum();
        let mut id_pool = BytesMut::with_capacity(id_bytes);
        for r in chunk {
            id_pool.extend_from_slice(r.user_id.as_slice());
        }
        let mut id_pool = id_pool.freeze();
        for r in chunk {
            self.user_ids
                .push(ByteArray::from(id_pool.split_to(r.user_id.as_slice().len())));
            self.days.push(r.day);
            self.sessions.push(r.sessions);
            self.events.push(r.events);
            self.revenues.push(r.revenue_cents);
            match r.top_category {
                Some(cid) => {
                    self.cat_defs.push(1i16);
                    self.cat_vals
                        .push(ByteArray::from(self.cat_bytes[cid as usize].clone()));
                }
                None => self.cat_defs.push(0i16),
            }
        }
        let mut rg = self.writer.next_row_group().map_err(perr)?;
        {
            let mut col = rg.next_column().map_err(perr)?.unwrap();
            col.typed::<ByteArrayType>()
                .write_batch(&self.user_ids, None, None)
                .map_err(perr)?;
            col.close().map_err(perr)?;
        }
        {
            let mut col = rg.next_column().map_err(perr)?.unwrap();
            col.typed::<Int32Type>()
                .write_batch(&self.days, None, None)
                .map_err(perr)?;
            col.close().map_err(perr)?;
        }
        for arr in [&self.sessions, &self.events, &self.revenues] {
            let mut col = rg.next_column().map_err(perr)?.unwrap();
            col.typed::<Int64Type>()
                .write_batch(arr, None, None)
                .map_err(perr)?;
            col.close().map_err(perr)?;
        }
        {
            let mut col = rg.next_column().map_err(perr)?.unwrap();
            col.typed::<ByteArrayType>()
                .write_batch(&self.cat_vals, Some(&self.cat_defs), None)
                .map_err(perr)?;
            col.close().map_err(perr)?;
        }
        rg.close().map_err(perr)?;
        Ok(())
    }

    pub fn close(self) -> io::Result<()> {
        self.writer.close().map_err(perr)?;
        Ok(())
    }
}

pub fn write_user_daily_part(
    path: &Path,
    parts: &[&[UserDailyRow]],
    categories: &[Vec<u8>],
) -> io::Result<()> {
    let mut w = UserDailyWriter::create(path, categories)?;
    for chunk in parts.iter().flat_map(|p| p.chunks(BATCH_ROWS)) {
        w.write_batch(chunk)?;
    }
    w.close()
}

pub fn write_campaign_daily(out_dir: &Path, rows: &[CampaignDailyRow]) -> io::Result<()> {
    let dir = out_dir.join("campaign_daily");
    fs::create_dir_all(&dir)?;
    let path = dir.join("part-0.parquet");
    let file = BufWriter::with_capacity(1 << 20, File::create(path)?);
    let schema = campaign_daily_schema();
    let props = build_writer_props();
    let mut writer = SerializedFileWriter::new(file, schema, props)
        .map_err(|e| io::Error::other(e.to_string()))?;

    if !rows.is_empty() {
        for chunk in rows.chunks(BATCH_ROWS) {
            let cids: Vec<ByteArray> = chunk
                .iter()
                .map(|r| ByteArray::from(Bytes::copy_from_slice(&r.campaign_id)))
                .collect();
            let chans: Vec<ByteArray> = chunk
                .iter()
                .map(|r| ByteArray::from(Bytes::copy_from_slice(&r.channel)))
                .collect();
            let days: Vec<i32> = chunk.iter().map(|r| r.day).collect();
            let sess: Vec<i64> = chunk.iter().map(|r| r.sessions).collect();
            let psess: Vec<i64> = chunk.iter().map(|r| r.purchasing_sessions).collect();
            let revs: Vec<i64> = chunk.iter().map(|r| r.revenue_cents).collect();
            let assts: Vec<i64> = chunk.iter().map(|r| r.assisted_sessions).collect();

            let mut rg = writer
                .next_row_group()
                .map_err(|e| io::Error::other(e.to_string()))?;

            for arr in [&cids, &chans] {
                let mut col = rg
                    .next_column()
                    .map_err(|e| io::Error::other(e.to_string()))?
                    .unwrap();
                col.typed::<ByteArrayType>()
                    .write_batch(arr, None, None)
                    .map_err(|e| io::Error::other(e.to_string()))?;
                col.close().map_err(|e| io::Error::other(e.to_string()))?;
            }
            {
                let mut col = rg
                    .next_column()
                    .map_err(|e| io::Error::other(e.to_string()))?
                    .unwrap();
                col.typed::<Int32Type>()
                    .write_batch(&days, None, None)
                    .map_err(|e| io::Error::other(e.to_string()))?;
                col.close().map_err(|e| io::Error::other(e.to_string()))?;
            }
            for arr in [&sess, &psess, &revs, &assts] {
                let mut col = rg
                    .next_column()
                    .map_err(|e| io::Error::other(e.to_string()))?
                    .unwrap();
                col.typed::<Int64Type>()
                    .write_batch(arr, None, None)
                    .map_err(|e| io::Error::other(e.to_string()))?;
                col.close().map_err(|e| io::Error::other(e.to_string()))?;
            }

            rg.close().map_err(|e| io::Error::other(e.to_string()))?;
        }
    }

    writer
        .close()
        .map_err(|e| io::Error::other(e.to_string()))?;
    Ok(())
}
