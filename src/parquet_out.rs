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

const BATCH_ROWS: usize = 65_536;

pub fn write_user_daily_part(
    path: &Path,
    parts: &[&[UserDailyRow]],
    categories: &[Vec<u8>],
) -> io::Result<()> {
    let file = BufWriter::with_capacity(1 << 20, File::create(path)?);
    let schema = user_daily_schema();
    let props = build_writer_props();
    let mut writer = SerializedFileWriter::new(file, schema, props)
        .map_err(|e| io::Error::other(e.to_string()))?;

    let cat_bytes: Vec<Bytes> = categories
        .iter()
        .map(|c| Bytes::copy_from_slice(c))
        .collect();
    let total: usize = parts.iter().map(|p| p.len()).sum();
    let cap = BATCH_ROWS.min(total);
    let mut user_ids = Vec::with_capacity(cap);
    let mut days = Vec::with_capacity(cap);
    let mut sessions = Vec::with_capacity(cap);
    let mut events = Vec::with_capacity(cap);
    let mut revenues = Vec::with_capacity(cap);
    let mut cat_defs = Vec::with_capacity(cap);
    let mut cat_vals = Vec::with_capacity(cap);

    for chunk in parts.iter().flat_map(|p| p.chunks(BATCH_ROWS)) {
        user_ids.clear();
        days.clear();
        sessions.clear();
        events.clear();
        revenues.clear();
        cat_defs.clear();
        cat_vals.clear();

        let mut id_pool = BytesMut::with_capacity(chunk.len() * 12);
        for r in chunk {
            id_pool.extend_from_slice(r.user_id.as_slice());
            user_ids.push(ByteArray::from(
                id_pool.split_to(r.user_id.as_slice().len()).freeze(),
            ));
            days.push(r.day);
            sessions.push(r.sessions);
            events.push(r.events);
            revenues.push(r.revenue_cents);
            match r.top_category {
                Some(cid) => {
                    cat_defs.push(1i16);
                    cat_vals.push(ByteArray::from(cat_bytes[cid as usize].clone()));
                }
                None => {
                    cat_defs.push(0i16);
                }
            }
        }

        let mut rg = writer
            .next_row_group()
            .map_err(|e| io::Error::other(e.to_string()))?;

        {
            let mut col = rg
                .next_column()
                .map_err(|e| io::Error::other(e.to_string()))?
                .unwrap();
            col.typed::<ByteArrayType>()
                .write_batch(&user_ids, None, None)
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
        {
            let mut col = rg
                .next_column()
                .map_err(|e| io::Error::other(e.to_string()))?
                .unwrap();
            col.typed::<Int64Type>()
                .write_batch(&sessions, None, None)
                .map_err(|e| io::Error::other(e.to_string()))?;
            col.close().map_err(|e| io::Error::other(e.to_string()))?;
        }
        {
            let mut col = rg
                .next_column()
                .map_err(|e| io::Error::other(e.to_string()))?
                .unwrap();
            col.typed::<Int64Type>()
                .write_batch(&events, None, None)
                .map_err(|e| io::Error::other(e.to_string()))?;
            col.close().map_err(|e| io::Error::other(e.to_string()))?;
        }
        {
            let mut col = rg
                .next_column()
                .map_err(|e| io::Error::other(e.to_string()))?
                .unwrap();
            col.typed::<Int64Type>()
                .write_batch(&revenues, None, None)
                .map_err(|e| io::Error::other(e.to_string()))?;
            col.close().map_err(|e| io::Error::other(e.to_string()))?;
        }
        {
            let mut col = rg
                .next_column()
                .map_err(|e| io::Error::other(e.to_string()))?
                .unwrap();
            col.typed::<ByteArrayType>()
                .write_batch(&cat_vals, Some(&cat_defs), None)
                .map_err(|e| io::Error::other(e.to_string()))?;
            col.close().map_err(|e| io::Error::other(e.to_string()))?;
        }

        rg.close().map_err(|e| io::Error::other(e.to_string()))?;
    }

    writer
        .close()
        .map_err(|e| io::Error::other(e.to_string()))?;
    Ok(())
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
