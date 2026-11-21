#[inline]
pub fn is_valid_utf8(bytes: &[u8]) -> bool {
    if bytes.is_ascii() {
        return true;
    }
    std::str::from_utf8(bytes).is_ok()
}

#[inline]
pub fn unquote_field(raw: &[u8]) -> Option<&[u8]> {
    if raw.is_empty() {
        return Some(&[]);
    }
    if raw[0] == b'"' {
        if raw.len() < 2 || raw[raw.len() - 1] != b'"' {
            return None;
        }
        let inner = &raw[1..raw.len() - 1];
        if memchr::memchr(b'"', inner).is_some() || !is_valid_utf8(inner) {
            return None;
        }
        Some(inner)
    } else {
        if memchr::memchr(b'"', raw).is_some() || !is_valid_utf8(raw) {
            return None;
        }
        Some(raw)
    }
}

#[inline]
pub fn parse_fixed_fields<'a, const N: usize>(mut line: &'a [u8]) -> Option<[&'a [u8]; N]> {
    if line.last() == Some(&b'\n') {
        line = &line[..line.len() - 1];
    }
    if line.is_empty() {
        return None;
    }
    let mut out = [&[][..]; N];
    let mut start = 0usize;
    for slot in out.iter_mut().take(N - 1) {
        let rel = memchr::memchr(b',', &line[start..])?;
        *slot = unquote_field(&line[start..start + rel])?;
        start += rel + 1;
    }
    let tail = &line[start..];
    if memchr::memchr(b',', tail).is_some() {
        return None;
    }
    out[N - 1] = unquote_field(tail)?;
    Some(out)
}

/// Fast path for lines in an ASCII-verified chunk with no `"` anywhere:
/// fields are returned raw, with only comma-count validation.
#[inline]
pub fn split_fields_unquoted<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    if line.is_empty() {
        return None;
    }
    let mut out = [&[][..]; N];
    let mut start = 0usize;
    for slot in out.iter_mut().take(N - 1) {
        let rel = memchr::memchr(b',', &line[start..])?;
        *slot = &line[start..start + rel];
        start += rel + 1;
    }
    let tail = &line[start..];
    if memchr::memchr(b',', tail).is_some() {
        return None;
    }
    out[N - 1] = tail;
    Some(out)
}

#[inline]
pub fn parse_price_cents(raw: &[u8]) -> Option<u32> {
    if raw.is_empty() {
        return None;
    }
    let mut acc: u64 = 0;
    for &b in raw {
        let d = b.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        acc = acc * 10 + u64::from(d);
        if acc > 10_000_000 {
            return None;
        }
    }
    if acc == 0 {
        return None;
    }
    Some(acc as u32)
}

pub fn iter_data_lines(bytes: &[u8], mut f: impl FnMut(&[u8])) {
    let Some(first_nl) = memchr::memchr(b'\n', bytes) else {
        return;
    };
    let mut pos = first_nl + 1;
    while pos < bytes.len() {
        match memchr::memchr(b'\n', &bytes[pos..]) {
            Some(rel) => {
                f(&bytes[pos..pos + rel]);
                pos += rel + 1;
            }
            None => {
                f(&bytes[pos..]);
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn csv_dialect_traps() {
        assert_eq!(parse_fixed_fields::<2>(b"\"u1\",\"u2\"\n"), Some([&b"u1"[..], &b"u2"[..]]));
        assert_eq!(parse_fixed_fields::<2>(b"\"\",u2\n"), Some([&b""[..], &b"u2"[..]]));
        assert_eq!(parse_fixed_fields::<2>(b" u1,\" u1\"\n"), Some([&b" u1"[..], &b" u1"[..]]));
        assert_eq!(parse_fixed_fields::<2>(b"u1\r,u2\r\n"), Some([&b"u1\r"[..], &b"u2\r"[..]]));
        assert_eq!(parse_fixed_fields::<2>(b"\n"), None);
        assert_eq!(parse_fixed_fields::<2>(b""), None);
        assert_eq!(parse_fixed_fields::<2>(b"u1,u2,u3\n"), None);
        assert_eq!(parse_fixed_fields::<2>(b"u1\n"), None);
        assert_eq!(parse_fixed_fields::<2>(b"\"u1,u2\n"), None);
        assert_eq!(parse_fixed_fields::<2>(b"u\"1,u2\n"), None);
        assert_eq!(parse_fixed_fields::<2>(b"\"u\"\"1\",u2\n"), None);
    }

    #[test]
    fn price_cents_traps() {
        assert_eq!(parse_price_cents(b"1"), Some(1));
        assert_eq!(parse_price_cents(b"0250"), Some(250));
        assert_eq!(parse_price_cents(b"10000000"), Some(10_000_000));
        assert_eq!(parse_price_cents(b"0"), None);
        assert_eq!(parse_price_cents(b"00"), None);
        assert_eq!(parse_price_cents(b"10000001"), None);
        assert_eq!(parse_price_cents(b"1000\r"), None);
        assert_eq!(parse_price_cents(b"+500"), None);
        assert_eq!(parse_price_cents(b"-5"), None);
        assert_eq!(parse_price_cents(b"19.99"), None);
        assert_eq!(parse_price_cents("٢٥٠".as_bytes()), None);
        assert_eq!(parse_price_cents("２５０".as_bytes()), None);
    }
}
