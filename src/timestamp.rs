#[inline]
pub fn is_leap_year(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

#[inline]
pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

#[inline]
fn parse_2d(b: &[u8], idx: usize) -> Option<i64> {
    let d0 = b[idx].wrapping_sub(b'0');
    let d1 = b[idx + 1].wrapping_sub(b'0');
    if d0 > 9 || d1 > 9 {
        return None;
    }
    Some(i64::from(d0 * 10 + d1))
}

#[inline]
fn parse_4d(b: &[u8], idx: usize) -> Option<i64> {
    let d0 = b[idx].wrapping_sub(b'0');
    let d1 = b[idx + 1].wrapping_sub(b'0');
    let d2 = b[idx + 2].wrapping_sub(b'0');
    let d3 = b[idx + 3].wrapping_sub(b'0');
    if d0 > 9 || d1 > 9 || d2 > 9 || d3 > 9 {
        return None;
    }
    Some(i64::from(d0) * 1000 + i64::from(d1) * 100 + i64::from(d2) * 10 + i64::from(d3))
}

#[inline]
pub fn validate_and_days(year: i64, month: i64, day: i64) -> Option<i64> {
    if !(1..=12).contains(&month) || day < 1 {
        return None;
    }
    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(year) {
                29
            } else {
                28
            }
        }
        _ => unreachable!(),
    };
    if day > max_day {
        return None;
    }
    Some(days_from_civil(year, month, day))
}

#[inline]
fn parse_civil_prefix(b: &[u8]) -> Option<i64> {
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let year = parse_4d(b, 0)?;
    if year == 0 {
        return None;
    }
    let month = parse_2d(b, 5)?;
    let day = parse_2d(b, 8)?;
    let hour = parse_2d(b, 11)?;
    let minute = parse_2d(b, 14)?;
    let second = parse_2d(b, 17)?;
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let days = validate_and_days(year, month, day)?;
    Some((days * 86_400 + hour * 3_600 + minute * 60 + second) * 1_000)
}

#[inline]
pub fn parse_timestamp(b: &[u8]) -> Option<i64> {
    if b.is_empty() {
        return None;
    }
    if b.len() == 20 && b[19] == b'Z' {
        return parse_civil_prefix(b);
    }
    if b.len() == 25 && (b[19] == b'+' || b[19] == b'-') && b[22] == b':' {
        let local_ms = parse_civil_prefix(b)?;
        let oh = parse_2d(b, 20)?;
        let om = parse_2d(b, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        let offset_ms = (oh * 3_600 + om * 60) * 1_000;
        return Some(if b[19] == b'+' {
            local_ms - offset_ms
        } else {
            local_ms + offset_ms
        });
    }
    let mut acc: i64 = 0;
    let mut overflowed = false;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        if !overflowed {
            match acc.checked_mul(10).and_then(|v| v.checked_add(i64::from(d))) {
                Some(next) => acc = next,
                None => overflowed = true,
            }
        }
    }
    Some(if overflowed { i64::MAX } else { acc })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn f1_f2_f3_and_bad_forms() {
        let t1 = parse_timestamp(b"2026-10-03T23:58:10Z").unwrap();
        assert_eq!(t1, 1_791_071_890_000);
        assert_eq!(parse_timestamp(b"2026-10-03T19:58:10-04:00"), Some(t1));
        assert_eq!(parse_timestamp(b"2026-10-03T23:58:10-00:00"), Some(t1));
        assert_eq!(parse_timestamp(b"1791071890000"), Some(t1));
        assert_eq!(parse_timestamp(b"0001791071890000"), Some(t1));
        assert_eq!(parse_timestamp(b"86400000"), Some(86_400_000));
        assert_eq!(parse_timestamp(b"179102160000000000000"), Some(i64::MAX));

        assert!(parse_timestamp(b"2028-02-29T00:00:00Z").is_some());
        assert_eq!(parse_timestamp(b"2027-02-29T00:00:00Z"), None);
        assert_eq!(parse_timestamp(b"2026-02-30T00:00:00Z"), None);
        assert_eq!(parse_timestamp(b"2026-13-01T00:00:00Z"), None);
        assert_eq!(parse_timestamp(b"2026-10-03T24:00:00Z"), None);
        assert_eq!(parse_timestamp(b"2026-10-03T23:58:60Z"), None);
        assert_eq!(parse_timestamp(b"2026-10-03t23:58:10Z"), None);
        assert_eq!(parse_timestamp(b"2026-10-03T23:58:10z"), None);
        assert_eq!(parse_timestamp(b"2026-10-03 23:58:10Z"), None);
        assert_eq!(parse_timestamp(b"2026-10-03T23:58:10.000Z"), None);
        assert_eq!(parse_timestamp(b"2026-10-03T23:58:10"), None);
        assert_eq!(parse_timestamp(b"2026-10-03T10:00:00+24:00"), None);
        assert_eq!(parse_timestamp(b"2026-10-03T10:00:00+05:60"), None);
        assert_eq!(parse_timestamp(b"2026-10-03T10:00:00+0200"), None);
        assert_eq!(parse_timestamp(b"2026-10-03T+1:+2:+3++1:+2"), None);
        assert_eq!(parse_timestamp(b"2026-10-03T23:58:10Z\r"), None);
        assert_eq!(parse_timestamp(b"1791071890000.0"), None);
        assert_eq!(parse_timestamp(b"-5"), None);
        assert_eq!(parse_timestamp(b""), None);
        assert_eq!(parse_timestamp("١٧٩١٠٢١٦٠٠٠٠٠".as_bytes()), None);
    }
}
