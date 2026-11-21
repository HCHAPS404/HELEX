use std::collections::HashMap;
use std::env;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

struct UnionFind {
    ids: HashMap<String, u32>,
    strings: Vec<String>,
    parent: Vec<u32>,
    size: Vec<u32>,
    minimum: Vec<u32>,
}

impl UnionFind {
    fn new() -> Self {
        Self {
            ids: HashMap::new(),
            strings: Vec::new(),
            parent: Vec::new(),
            size: Vec::new(),
            minimum: Vec::new(),
        }
    }

    fn add(&mut self, s: &str) -> u32 {
        if let Some(&index) = self.ids.get(s) {
            return index;
        }

        let index = self.strings.len() as u32;
        self.ids.insert(s.to_string(), index);
        self.strings.push(s.to_string());
        self.parent.push(index);
        self.size.push(1);
        self.minimum.push(index);
        index
    }

    fn find_index(&mut self, index: u32) -> u32 {
        let mut root = index;
        while self.parent[root as usize] != root {
            root = self.parent[root as usize];
        }

        let mut cur = index;
        while self.parent[cur as usize] != cur {
            let next = self.parent[cur as usize];
            self.parent[cur as usize] = root;
            cur = next;
        }
        root
    }

    fn find(&mut self, s: &str) -> u32 {
        let index = self.add(s);
        self.find_index(index)
    }

    fn union(&mut self, a: &str, b: &str) {
        let ra = self.find(a);
        let rb = self.find(b);
        if ra == rb { return; }
        let sa = self.size[ra as usize];
        let sb = self.size[rb as usize];

        let (root, child) = if sa < sb { (rb, ra) } else { (ra, rb) };
        self.parent[child as usize] = root;
        self.size[root as usize] = sa + sb;

        let current_min = self.minimum[root as usize];
        let child_min = self.minimum[child as usize];
        if self.strings[child_min as usize].as_bytes() < self.strings[current_min as usize].as_bytes() {
            self.minimum[root as usize] = child_min;
        }
    }

    fn name(&mut self, value: &str) -> String {
        let root = self.find(value);
        self.strings[self.minimum[root as usize] as usize].clone()
    }

    #[cfg(test)]
    fn component_count(&mut self) -> usize {
        let mut count = 0;
        for index in 0..self.strings.len() as u32 {
            if self.find_index(index) == index {
                count += 1;
            }
        }
        count
    }
}

fn parse_field(raw: &[u8]) -> Option<String> {
    if raw.is_empty() { return Some(String::new()); }
    if !raw.contains(&b'"') {
        return std::str::from_utf8(raw).ok().map(|s| s.to_string());
    }
    if raw.len() < 2 || raw[0] != b'"' || raw[raw.len()-1] != b'"' {
        return None;
    }
    let inner = &raw[1..raw.len()-1];
    if inner.contains(&b'"') { return None; }
    std::str::from_utf8(inner).ok().map(|s| s.to_string())
}

fn parse_line(line: &[u8], count: usize) -> Option<Vec<String>> {
    let line = if let Some(x) = line.strip_suffix(b"\n") { x } else { line };
    if line.is_empty() { return None; }
    let parts: Vec<&[u8]> = line.split(|b| *b == b',').collect();
    if parts.len() != count { return None; }
    parts.into_iter().map(parse_field).collect()
}

fn leap(y: i64) -> bool { y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) }
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = y - if m <= 2 { 1 } else { 0 };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = m + if m > 2 { -3 } else { 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}
fn calendar_ms(y: i64, m: i64, d: i64, h: i64, mi: i64, s: i64) -> Option<i64> {
    if !(1..=9999).contains(&y) || !(1..=12).contains(&m) ||
       !(0..=23).contains(&h) || !(0..=59).contains(&mi) || !(0..=59).contains(&s) {
        return None;
    }
    let md = [31, if leap(y){29}else{28},31,30,31,30,31,31,30,31,30,31];
    if d < 1 || d > md[(m-1) as usize] { return None; }
    let days = days_from_civil(y,m,d);
    Some((days * 86400 + h*3600 + mi*60 + s)*1000)
}
fn ascii_digits(s: &str) -> bool { !s.is_empty() && s.bytes().all(|b| b >= b'0' && b <= b'9') }
fn parse_timestamp(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    if b.len() == 20 && b[4]==b'-' && b[7]==b'-' && b[10]==b'T' && b[13]==b':' && b[16]==b':' && b[19]==b'Z' &&
       b.iter().enumerate().all(|(i,&c)| [4,7,10,13,16,19].contains(&i) || c.is_ascii_digit()) {
        let y=s[0..4].parse().ok()?; let m=s[5..7].parse().ok()?; let d=s[8..10].parse().ok()?;
        let h=s[11..13].parse().ok()?; let mi=s[14..16].parse().ok()?; let se=s[17..19].parse().ok()?;
        return calendar_ms(y,m,d,h,mi,se);
    }
    if b.len() == 25 && b[4]==b'-' && b[7]==b'-' && b[10]==b'T' && b[13]==b':' && b[16]==b':' &&
       (b[19]==b'+' || b[19]==b'-') && b[22]==b':' {
        let y=s[0..4].parse().ok()?; let m=s[5..7].parse().ok()?; let d=s[8..10].parse().ok()?;
        let h=s[11..13].parse().ok()?; let mi=s[14..16].parse().ok()?; let se=s[17..19].parse().ok()?;
        let oh=s[20..22].parse::<i64>().ok()?; let om=s[23..25].parse::<i64>().ok()?;
        if oh > 23 || om > 59 { return None; }
        let mut ms = calendar_ms(y,m,d,h,mi,se)?;
        let off=(oh*3600+om*60)*1000;
        ms = if b[19]==b'+' { ms-off } else { ms+off };
        return Some(ms);
    }
    if ascii_digits(s) {
        if let Ok(n)=s.parse::<i64>() { return Some(n); }
        return Some(i64::MAX);
    }
    None
}

fn csv_paths(folder: &Path) -> Vec<PathBuf> {
    let mut out: Vec<_> = fs::read_dir(folder).ok().into_iter().flatten()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().and_then(|x| x.to_str())==Some("csv"))
        .collect();
    out.sort();
    out
}

fn write_field(out: &mut impl Write, value: &str) -> std::io::Result<()> {
    if value.bytes().any(|b| b == b',' || b == b'"' || b == b'\r' || b == b'\n') {
        out.write_all(b"\"")?;
        for b in value.bytes() {
            if b == b'"' {
                out.write_all(b"\"\"")?;
            } else {
                out.write_all(&[b])?;
            }
        }
        out.write_all(b"\"")?;
    } else {
        out.write_all(value.as_bytes())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::UnionFind;

    #[test]
    fn keeps_bytewise_minimum_across_chain() {
        let mut uf = UnionFind::new();
        uf.union("U1", "u2");
        uf.union("u2", "u3");
        uf.union("u3", "A0");

        assert_eq!(uf.name("u3"), "A0");
        assert_eq!(uf.name("U1"), "A0");
        assert_eq!(uf.name("absent"), "absent");
        assert_eq!(uf.component_count(), 2);
    }

    #[test]
    fn ghost_id_can_be_the_person_name() {
        let mut uf = UnionFind::new();
        uf.union("user-z", "user-m");
        uf.union("A-ghost", "user-z");

        assert_eq!(uf.name("user-m"), "A-ghost");
    }

    #[test]
    fn long_chain_with_path_compression() {
        let mut uf = UnionFind::new();
        for i in 0..400usize {
            uf.union(&format!("id-{i:04}"), "chain-root");
        }

        assert_eq!(uf.component_count(), 1);
        assert_eq!(uf.name("id-0399"), "chain-root");
    }

    #[test]
    fn million_id_scale_smoke_test() {
        let mut uf = UnionFind::new();
        for i in 0..100_000usize {
            uf.union(&format!("user-{i:06}"), "shared-device");
        }
        assert_eq!(uf.component_count(), 1);
        assert_eq!(uf.name("user-099999"), "shared-device");
    }
}

fn main() -> std::io::Result<()> {
    let data = env::var("DATA_DIR").unwrap_or_else(|_| "/data".into());
    let scratch = env::var("SCRATCH_DIR").unwrap_or_else(|_| "/scratch".into());
    fs::create_dir_all(&scratch)?;

    let mut uf = UnionFind::new();
    for path in csv_paths(&Path::new(&data).join("links")) {
        let file=File::open(path)?;
        let mut reader=BufReader::new(file);
        let mut line=Vec::new(); reader.read_until(b'\n', &mut line)?; line.clear();
        loop {
            line.clear();
            if reader.read_until(b'\n', &mut line)? == 0 { break; }
            if let Some(f)=parse_line(&line,2) {
                if !f[0].is_empty() && !f[1].is_empty() { uf.union(&f[0],&f[1]); }
            }
        }
    }
    let mut canonical = |id: &str| -> String {
        if id.is_empty() { return String::new(); }
        uf.name(id)
    };

    let event_out=Path::new(&scratch).join("shopping_staging/events.csv");
    fs::create_dir_all(event_out.parent().unwrap())?;
    let mut ev=BufWriter::new(File::create(&event_out)?);
    writeln!(ev,"event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents")?;
    for path in csv_paths(&Path::new(&data).join("events")) {
        let file=File::open(path)?;
        let mut reader=BufReader::new(file);
        let mut line=Vec::new(); reader.read_until(b'\n', &mut line)?;
        loop {
            line.clear();
            if reader.read_until(b'\n', &mut line)? == 0 { break; }
            if let Some(f)=parse_line(&line,7) {
                if f[0].is_empty() { continue; }
                let ts=parse_timestamp(&f[2]).map(|x|x.to_string()).unwrap_or_default();
                let ingest=parse_timestamp(&f[3]).map(|x|x.to_string()).unwrap_or_default();
                let user=canonical(&f[1]);
                let values=[f[0].as_str(), user.as_str(), ts.as_str(), ingest.as_str(), f[4].as_str(), f[5].as_str(), f[6].as_str()];
                for (i,value) in values.iter().enumerate() {
                    if i > 0 { write!(ev,",")?; }
                    write_field(&mut ev,value)?;
                }
                writeln!(ev)?;
            }
        }
    }

    let click_out=Path::new(&scratch).join("shopping_staging/clicks.csv");
    let mut cl=BufWriter::new(File::create(&click_out)?);
    writeln!(cl,"click_id,user_id,campaign_id,ts")?;
    for path in csv_paths(&Path::new(&data).join("clicks")) {
        let file=File::open(path)?;
        let mut reader=BufReader::new(file);
        let mut line=Vec::new(); reader.read_until(b'\n', &mut line)?;
        loop {
            line.clear();
            if reader.read_until(b'\n', &mut line)? == 0 { break; }
            if let Some(f)=parse_line(&line,4) {
                if f[0].is_empty() { continue; }
                let ts=parse_timestamp(&f[3]).map(|x|x.to_string()).unwrap_or_default();
                let user=canonical(&f[1]);
                let values=[f[0].as_str(), user.as_str(), f[2].as_str(), ts.as_str()];
                for (i,value) in values.iter().enumerate() {
                    if i > 0 { write!(cl,",")?; }
                    write_field(&mut cl,value)?;
                }
                writeln!(cl)?;
            }
        }
    }
    Ok(())
}
