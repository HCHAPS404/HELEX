//! Minimal Parquet writer for flat schemas: one v1 DATA_PAGE per column chunk,
//! PLAIN values, UNCOMPRESSED, no dictionary, no statistics. Optional columns
//! carry a bit-packed definition-level run. Metadata is Thrift compact.

use std::fs::File;
use std::io::{self, Write};
use std::path::Path;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// required BYTE_ARRAY (String)
    Utf8,
    /// optional BYTE_ARRAY (String)
    Utf8Opt,
    /// required INT32 (Date)
    Date,
    /// required INT64
    I64,
}

pub struct ColDef {
    pub name: &'static str,
    pub kind: Kind,
}

/// Column buffer: PLAIN-encoded values plus definition levels for optional
/// columns.
#[derive(Default)]
pub struct Col {
    pub data: Vec<u8>,
    defs: Vec<u8>, // bit-packed, LSB first
    pub n: usize,
}

impl Col {
    #[inline(always)]
    pub fn push_bytes(&mut self, v: &[u8]) {
        self.data.extend_from_slice(&(v.len() as u32).to_le_bytes());
        self.data.extend_from_slice(v);
        self.n += 1;
    }
    #[inline(always)]
    pub fn push_opt_bytes(&mut self, v: Option<&[u8]>) {
        if self.n % 8 == 0 {
            self.defs.push(0);
        }
        if let Some(v) = v {
            *self.defs.last_mut().unwrap() |= 1 << (self.n % 8);
            self.data.extend_from_slice(&(v.len() as u32).to_le_bytes());
            self.data.extend_from_slice(v);
        }
        self.n += 1;
    }
    #[inline(always)]
    pub fn push_i32(&mut self, v: i32) {
        self.data.extend_from_slice(&v.to_le_bytes());
        self.n += 1;
    }
    #[inline(always)]
    pub fn push_i64(&mut self, v: i64) {
        self.data.extend_from_slice(&v.to_le_bytes());
        self.n += 1;
    }
    fn clear(&mut self) {
        self.data.clear();
        self.defs.clear();
        self.n = 0;
    }
}

// ---------------------------------------------------------- thrift compact

struct Tc {
    out: Vec<u8>,
    last: Vec<i16>,
}

const T_TRUE: u8 = 1;
const T_I32: u8 = 5;
const T_I64: u8 = 6;
const T_BIN: u8 = 8;
const T_LIST: u8 = 9;
const T_STRUCT: u8 = 12;

impl Tc {
    fn new() -> Self {
        Self {
            out: Vec::with_capacity(512),
            last: vec![0],
        }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.out.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.out.push(v as u8);
    }
    fn zz(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn field(&mut self, id: i16, ty: u8) {
        let last = *self.last.last().unwrap();
        let d = id - last;
        if d > 0 && d <= 15 {
            self.out.push(((d as u8) << 4) | ty);
        } else {
            self.out.push(ty);
            self.zz(i64::from(id));
        }
        *self.last.last_mut().unwrap() = id;
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.field(id, T_I32);
        self.zz(i64::from(v));
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.field(id, T_I64);
        self.zz(v);
    }
    fn bin(&mut self, id: i16, v: &[u8]) {
        self.field(id, T_BIN);
        self.varint(v.len() as u64);
        self.out.extend_from_slice(v);
    }
    fn begin(&mut self, id: i16) {
        self.field(id, T_STRUCT);
        self.last.push(0);
    }
    /// struct element inside a list (no field header)
    fn begin_elem(&mut self) {
        self.last.push(0);
    }
    fn end(&mut self) {
        self.out.push(0);
        self.last.pop();
    }
    fn list(&mut self, id: i16, elem: u8, n: usize) {
        self.field(id, T_LIST);
        if n < 15 {
            self.out.push(((n as u8) << 4) | elem);
        } else {
            self.out.push(0xF0 | elem);
            self.varint(n as u64);
        }
    }
    fn elem_i32(&mut self, v: i32) {
        self.zz(i64::from(v));
    }
    fn elem_bin(&mut self, v: &[u8]) {
        self.varint(v.len() as u64);
        self.out.extend_from_slice(v);
    }
}

const PT_INT32: i32 = 1;
const PT_INT64: i32 = 2;
const PT_BYTE_ARRAY: i32 = 6;
const ENC_PLAIN: i32 = 0;
const ENC_RLE: i32 = 3;

fn phys(k: Kind) -> i32 {
    match k {
        Kind::Utf8 | Kind::Utf8Opt => PT_BYTE_ARRAY,
        Kind::Date => PT_INT32,
        Kind::I64 => PT_INT64,
    }
}

struct ChunkMeta {
    offset: u64,
    size: u64,
    n: usize,
}

struct RgMeta {
    rows: usize,
    bytes: u64,
    cols: Vec<ChunkMeta>,
}

pub struct Writer {
    file: io::BufWriter<File>,
    schema_name: &'static str,
    defs: &'static [ColDef],
    pub cols: Vec<Col>,
    pos: u64,
    rgs: Vec<RgMeta>,
    total_rows: usize,
}

impl Writer {
    pub fn create(path: &Path, schema_name: &'static str, defs: &'static [ColDef]) -> io::Result<Self> {
        let mut file = io::BufWriter::with_capacity(1 << 20, File::create(path)?);
        file.write_all(b"PAR1")?;
        Ok(Self {
            file,
            schema_name,
            defs,
            cols: (0..defs.len()).map(|_| Col::default()).collect(),
            pos: 4,
            rgs: Vec::new(),
            total_rows: 0,
        })
    }

    pub fn rows(&self) -> usize {
        self.cols.first().map_or(0, |c| c.n)
    }

    /// Writes the buffered rows as one row group.
    pub fn flush_row_group(&mut self) -> io::Result<()> {
        let rows = self.rows();
        if rows == 0 {
            return Ok(());
        }
        let mut metas = Vec::with_capacity(self.defs.len());
        let mut rg_bytes = 0u64;
        for (d, c) in self.defs.iter().zip(self.cols.iter()) {
            debug_assert_eq!(c.n, rows);
            // page body: [def levels] + values
            let mut def_block: Vec<u8> = Vec::new();
            if d.kind == Kind::Utf8Opt {
                // RLE/bit-packed hybrid, one bit-packed run of ceil(n/8) groups
                let groups = c.n.div_ceil(8);
                let mut hdr = Tc::new();
                hdr.varint(((groups as u64) << 1) | 1);
                let mut run = hdr.out;
                run.extend_from_slice(&c.defs[..groups]);
                def_block.extend_from_slice(&(run.len() as u32).to_le_bytes());
                def_block.extend_from_slice(&run);
            }
            let body_len = def_block.len() + c.data.len();
            let mut ph = Tc::new();
            ph.i32(1, 0); // DATA_PAGE
            ph.i32(2, body_len as i32);
            ph.i32(3, body_len as i32);
            ph.begin(5);
            ph.i32(1, c.n as i32);
            ph.i32(2, ENC_PLAIN);
            ph.i32(3, ENC_RLE);
            ph.i32(4, ENC_RLE);
            ph.end();
            ph.out.push(0); // end PageHeader
            let offset = self.pos;
            self.file.write_all(&ph.out)?;
            self.file.write_all(&def_block)?;
            self.file.write_all(&c.data)?;
            let size = (ph.out.len() + body_len) as u64;
            self.pos += size;
            rg_bytes += size;
            metas.push(ChunkMeta { offset, size, n: c.n });
        }
        self.rgs.push(RgMeta {
            rows,
            bytes: rg_bytes,
            cols: metas,
        });
        self.total_rows += rows;
        for c in self.cols.iter_mut() {
            c.clear();
        }
        Ok(())
    }

    pub fn close(mut self) -> io::Result<usize> {
        self.flush_row_group()?;
        let mut t = Tc::new();
        t.i32(1, 1); // version
        t.list(2, T_STRUCT, self.defs.len() + 1);
        // root
        t.begin_elem();
        t.bin(4, self.schema_name.as_bytes());
        t.i32(5, self.defs.len() as i32);
        t.end();
        for d in self.defs {
            t.begin_elem();
            t.i32(1, phys(d.kind));
            t.i32(3, if d.kind == Kind::Utf8Opt { 1 } else { 0 });
            t.bin(4, d.name.as_bytes());
            match d.kind {
                Kind::Utf8 | Kind::Utf8Opt => {
                    t.i32(6, 0); // converted UTF8
                    t.begin(10); // logicalType
                    t.begin(1); // STRING
                    t.end();
                    t.end();
                }
                Kind::Date => {
                    t.i32(6, 6); // converted DATE
                    t.begin(10);
                    t.begin(6); // DATE
                    t.end();
                    t.end();
                }
                Kind::I64 => {}
            }
            t.end();
        }
        t.i64(3, self.total_rows as i64);
        t.list(4, T_STRUCT, self.rgs.len());
        for rg in &self.rgs {
            t.begin_elem();
            t.list(1, T_STRUCT, rg.cols.len());
            for (d, cm) in self.defs.iter().zip(rg.cols.iter()) {
                t.begin_elem();
                t.i64(2, cm.offset as i64);
                t.begin(3);
                t.i32(1, phys(d.kind));
                t.list(2, T_I32, 2);
                t.elem_i32(ENC_PLAIN);
                t.elem_i32(ENC_RLE);
                t.list(3, T_BIN, 1);
                t.elem_bin(d.name.as_bytes());
                t.i32(4, 0); // UNCOMPRESSED
                t.i64(5, cm.n as i64);
                t.i64(6, cm.size as i64);
                t.i64(7, cm.size as i64);
                t.i64(9, cm.offset as i64);
                t.end();
                t.end();
            }
            t.i64(2, rg.bytes as i64);
            t.i64(3, rg.rows as i64);
            t.end();
        }
        t.bin(6, b"helex");
        t.out.push(0);
        let _ = T_TRUE;
        self.file.write_all(&t.out)?;
        self.file.write_all(&(t.out.len() as u32).to_le_bytes())?;
        self.file.write_all(b"PAR1")?;
        self.file.flush()?;
        Ok(self.total_rows)
    }
}

pub static USER_DAILY: [ColDef; 6] = [
    ColDef { name: "user_id", kind: Kind::Utf8 },
    ColDef { name: "day", kind: Kind::Date },
    ColDef { name: "sessions", kind: Kind::I64 },
    ColDef { name: "events", kind: Kind::I64 },
    ColDef { name: "revenue_cents", kind: Kind::I64 },
    ColDef { name: "top_category", kind: Kind::Utf8Opt },
];

pub static CAMPAIGN_DAILY: [ColDef; 7] = [
    ColDef { name: "campaign_id", kind: Kind::Utf8 },
    ColDef { name: "channel", kind: Kind::Utf8 },
    ColDef { name: "day", kind: Kind::Date },
    ColDef { name: "sessions", kind: Kind::I64 },
    ColDef { name: "purchasing_sessions", kind: Kind::I64 },
    ColDef { name: "revenue_cents", kind: Kind::I64 },
    ColDef { name: "assisted_sessions", kind: Kind::I64 },
];
