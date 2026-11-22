#[inline]
pub const fn is_leap_year(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

#[inline]
pub const fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

#[inline]
pub const fn validate_and_days(year: i64, month: i64, day: i64) -> Option<i64> {
    if month < 1 || month > 12 || day < 1 {
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
        _ => return None,
    };
    if day > max_day {
        return None;
    }
    Some(days_from_civil(year, month, day))
}

const BASE_YEAR: usize = 2020;
const YEAR_SPAN: usize = 16;
const INVALID_DAY: i32 = i32::MIN;

const FAST_EPOCH_DAYS: [i32; YEAR_SPAN * 16 * 32] = {
    let mut table = [INVALID_DAY; YEAR_SPAN * 16 * 32];
    let mut y_idx = 0usize;
    while y_idx < YEAR_SPAN {
        let year = (BASE_YEAR + y_idx) as i64;
        let mut month = 1usize;
        while month <= 12 {
            let mut day = 1usize;
            while day <= 31 {
                if let Some(d) = validate_and_days(year, month as i64, day as i64) {
                    table[(y_idx << 9) | (month << 5) | day] = d as i32;
                }
                day += 1;
            }
            month += 1;
        }
        y_idx += 1;
    }
    table
};

#[inline]
fn parse_2d(b: &[u8], idx: usize) -> Option<u32> {
    let d0 = b[idx].wrapping_sub(b'0');
    let d1 = b[idx + 1].wrapping_sub(b'0');
    if (d0 | d1) > 9 && (d0 > 9 || d1 > 9) {
        return None;
    }
    Some(u32::from(d0) * 10 + u32::from(d1))
}

#[inline]
fn parse_4d(b: &[u8], idx: usize) -> Option<u32> {
    let d0 = b[idx].wrapping_sub(b'0');
    let d1 = b[idx + 1].wrapping_sub(b'0');
    let d2 = b[idx + 2].wrapping_sub(b'0');
    let d3 = b[idx + 3].wrapping_sub(b'0');
    if d0 > 9 || d1 > 9 || d2 > 9 || d3 > 9 {
        return None;
    }
    Some(u32::from(d0) * 1000 + u32::from(d1) * 100 + u32::from(d2) * 10 + u32::from(d3))
}

#[inline]
fn parse_civil_prefix(b: &[u8]) -> Option<i64> {
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let year = parse_4d(b, 0)?;
    let month = parse_2d(b, 5)?;
    let day = parse_2d(b, 8)?;
    let hour = parse_2d(b, 11)?;
    let minute = parse_2d(b, 14)?;
    let second = parse_2d(b, 17)?;
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }

    let days: i64 = {
        let y_off = (year as usize).wrapping_sub(BASE_YEAR);
        if y_off < YEAR_SPAN && month < 16 && day < 32 {
            let cached = FAST_EPOCH_DAYS[(y_off << 9) | ((month as usize) << 5) | (day as usize)];
            if cached == INVALID_DAY {
                return None;
            }
            i64::from(cached)
        } else {
            if year == 0 {
                return None;
            }
            validate_and_days(i64::from(year), i64::from(month), i64::from(day))?
        }
    };

    let tod = i64::from(hour * 3_600 + minute * 60 + second);
    Some((days * 86_400 + tod) * 1_000)
}

/// Per (year, month): (epoch day of the month's day 0, month length; 0 for
/// invalid months). 2 KiB instead of the 32 KiB per-day table, so it stays
/// in L1 next to the parse working set.
const MONTH_TABLE: [(i32, u32); YEAR_SPAN * 16] = {
    let mut t = [(0i32, 0u32); YEAR_SPAN * 16];
    let mut y = 0usize;
    while y < YEAR_SPAN {
        let mut m = 1usize;
        while m <= 12 {
            let year = (BASE_YEAR + y) as i64;
            let len = match m {
                1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
                4 | 6 | 9 | 11 => 30,
                _ => {
                    if is_leap_year(year) {
                        29
                    } else {
                        28
                    }
                }
            };
            t[(y << 4) | m] = ((days_from_civil(year, m as i64, 1) - 1) as i32, len);
            m += 1;
        }
        y += 1;
    }
    t
};

#[inline(always)]
fn days_of(year: u32, month: u32, day: u32) -> Option<i64> {
    let y_off = (year as usize).wrapping_sub(BASE_YEAR);
    if y_off < YEAR_SPAN && month < 16 {
        let (base, len) = MONTH_TABLE[(y_off << 4) | month as usize];
        if day.wrapping_sub(1) >= len {
            return None;
        }
        Some(i64::from(base) + i64::from(day))
    } else {
        if year == 0 {
            return None;
        }
        validate_and_days(i64::from(year), i64::from(month), i64::from(day))
    }
}

const ASCII0: u64 = 0x3030_3030_3030_3030;
const HI_NIB: u64 = 0xF0F0_F0F0_F0F0_F0F0;
const SIX: u64 = 0x0606_0606_0606_0606;

#[inline(always)]
fn digits_ok(d: u64, dm: u64) -> bool {
    let x = d & dm;
    (x & HI_NIB) == 0 && ((x + SIX) & HI_NIB) == 0
}

#[inline(always)]
fn byte(w: u64, i: u32) -> u32 {
    ((w >> (8 * i)) & 0xFF) as u32
}

/// SWAR parse of `YYYY-MM-DDTHH:MM:SS` + suffix byte at 19 (caller checked).
///
/// # Safety
/// `p..p+20` must be readable.
#[inline(always)]
unsafe fn parse_civil_swar(p: *const u8) -> Option<i64> {
    let w0 = std::ptr::read_unaligned(p as *const u64); // YYYY-MM-
    let w1 = std::ptr::read_unaligned(p.add(8) as *const u64); // DDTHH:MM
    let w2 = u64::from(std::ptr::read_unaligned(p.add(16) as *const u32)); // :SSx
                                                                           // separators
    const M0: u64 = 0xFF00_00FF_0000_0000; // bytes 4, 7
    const E0: u64 = 0x2D00_002D_0000_0000;
    const M1: u64 = 0x0000_FF00_00FF_0000; // bytes 2 'T', 5 ':'
    const E1: u64 = 0x0000_3A00_0054_0000;
    if (w0 & M0) != E0 || (w1 & M1) != E1 || (w2 & 0xFF) != 0x3A {
        return None;
    }
    let d0 = w0 ^ ASCII0;
    let d1 = w1 ^ ASCII0;
    let d2 = w2 ^ (ASCII0 & 0xFFFF_FFFF);
    const DM0: u64 = 0x00FF_FF00_FFFF_FFFF; // bytes 0-3, 5, 6
    const DM1: u64 = 0xFFFF_00FF_FF00_FFFF; // bytes 0,1,3,4,6,7
    const DM2: u64 = 0x0000_0000_00FF_FF00; // bytes 1,2
    if !(digits_ok(d0, DM0) && digits_ok(d1, DM1) && digits_ok(d2, DM2)) {
        return None;
    }
    let year = byte(d0, 0) * 1000 + byte(d0, 1) * 100 + byte(d0, 2) * 10 + byte(d0, 3);
    let month = byte(d0, 5) * 10 + byte(d0, 6);
    let day = byte(d1, 0) * 10 + byte(d1, 1);
    let hour = byte(d1, 3) * 10 + byte(d1, 4);
    let minute = byte(d1, 6) * 10 + byte(d1, 7);
    let second = byte(d2, 1) * 10 + byte(d2, 2);
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let days = days_of(year, month, day)?;
    let tod = i64::from(hour * 3_600 + minute * 60 + second);
    Some((days * 86_400 + tod) * 1_000)
}

/// Same result as [`parse_timestamp`], with a SWAR fast path for F1.
///
/// # Safety
/// At least 24 bytes from `b.as_ptr()` must be readable (padded buffer).
#[inline(always)]
pub unsafe fn parse_ts_padded(b: &[u8]) -> Option<i64> {
    let n = b.len();
    let p = b.as_ptr();
    if n == 20 && *p.add(19) == b'Z' {
        return parse_civil_swar(p);
    }
    if n >= 1 && n <= 16 && (*p).wrapping_sub(b'0') <= 9 {
        return parse_digits16(p, n);
    }
    if n == 25 {
        let sign = *p.add(19);
        if (sign == b'+' || sign == b'-') && *p.add(22) == b':' {
            let local_ms = parse_civil_swar(p)?;
            let oh = two_digits(p.add(20))?;
            let om = two_digits(p.add(23))?;
            if oh > 23 || om > 59 {
                return None;
            }
            let off = i64::from(oh * 3_600 + om * 60) * 1_000;
            return Some(if sign == b'+' { local_ms - off } else { local_ms + off });
        }
    }
    parse_timestamp(b)
}

#[inline(always)]
unsafe fn two_digits(p: *const u8) -> Option<u32> {
    let a = (*p).wrapping_sub(b'0');
    let b = (*p.add(1)).wrapping_sub(b'0');
    if a > 9 || b > 9 {
        return None;
    }
    Some(u32::from(a) * 10 + u32::from(b))
}

/// Value of 8 ASCII digits loaded little-endian (first char most significant).
#[inline(always)]
fn swar8(v: u64) -> u64 {
    let v = v.wrapping_sub(ASCII0);
    let v = (v.wrapping_mul(10) + (v >> 8)) & 0x00FF_00FF_00FF_00FF;
    let v = (v.wrapping_mul(100) + (v >> 16)) & 0x0000_FFFF_0000_FFFF;
    (v.wrapping_mul(10_000) + (v >> 32)) & 0xFFFF_FFFF
}

#[inline(always)]
fn all_digits8(v: u64) -> bool {
    let d = v ^ ASCII0;
    (d & HI_NIB) == 0 && ((d + SIX) & HI_NIB) == 0
}

/// F3: 1..=16 ASCII digits (value < 10^16, no overflow possible).
///
/// # Safety
/// 16 bytes from `p` must be readable.
#[inline(always)]
unsafe fn parse_digits16(p: *const u8, n: usize) -> Option<i64> {
    // Right-align the digits into two 8-digit words padded with '0'.
    let lo_n = n.min(8);
    let hi_n = n - lo_n;
    let lo_raw = std::ptr::read_unaligned(p.add(hi_n) as *const u64);
    let lo = if lo_n == 8 {
        lo_raw
    } else {
        (lo_raw << (8 * (8 - lo_n))) | (ASCII0 >> (8 * lo_n))
    };
    let hi = if hi_n == 0 {
        ASCII0
    } else {
        let raw = std::ptr::read_unaligned(p as *const u64);
        (raw << (8 * (8 - hi_n))) | (ASCII0 >> (8 * hi_n))
    };
    if !(all_digits8(lo) && all_digits8(hi)) {
        return None;
    }
    Some((swar8(hi) * 100_000_000 + swar8(lo)) as i64)
}

#[inline]
pub fn parse_timestamp(b: &[u8]) -> Option<i64> {
    if b.len() == 20 && b[19] == b'Z' {
        return parse_civil_prefix(b);
    }
    if b.is_empty() {
        return None;
    }
    if b.len() == 25 && (b[19] == b'+' || b[19] == b'-') && b[22] == b':' {
        let local_ms = parse_civil_prefix(b)?;
        let oh = parse_2d(b, 20)?;
        let om = parse_2d(b, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        let offset_ms = i64::from(oh * 3_600 + om * 60) * 1_000;
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
            match acc
                .checked_mul(10)
                .and_then(|v| v.checked_add(i64::from(d)))
            {
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
        assert!(parse_timestamp(b"2096-02-29T00:00:00Z").is_some());
        assert_eq!(parse_timestamp(b"2027-02-29T00:00:00Z"), None);
        assert_eq!(parse_timestamp(b"2100-02-29T00:00:00Z"), None);
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

    #[test]
    fn swar_matches_scalar() {
        let mut cases: Vec<Vec<u8>> = vec![
            b"1752993097682".to_vec(),
            b"0".to_vec(),
            b"7".to_vec(),
            b"00000000".to_vec(),
            b"123456789".to_vec(),
            b"9999999999999999".to_vec(),
            b"99999999999999999".to_vec(),
            b"1752993097682.0".to_vec(),
            b"17529930976a2".to_vec(),
            b"1.75e12".to_vec(),
            b"-5".to_vec(),
            b"2025-07-22T12:18:48+05:30".to_vec(),
            b"2025-07-22T12:18:48-00:00".to_vec(),
            b"2025-07-22T12:18:48+24:00".to_vec(),
            b"2025-07-22T12:18:48+05:60".to_vec(),
            b"2025-07-22T12:18:48+0530".to_vec(),
            b"2025-07-22T12:18:48*05:30".to_vec(),
            b"2025-07-22T12:18:48+0a:30".to_vec(),
            b"2026-10-03T23:58:10Z".to_vec(),
            b"2028-02-29T00:00:00Z".to_vec(),
            b"2027-02-29T00:00:00Z".to_vec(),
            b"2096-02-29T00:00:00Z".to_vec(),
            b"0000-01-01T00:00:00Z".to_vec(),
            b"9999-12-31T23:59:59Z".to_vec(),
            b"2026-10-03T24:00:00Z".to_vec(),
            b"2026-10-03T23:60:00Z".to_vec(),
            b"2026-10-03T23:58:60Z".to_vec(),
            b"2026-13-03T23:58:10Z".to_vec(),
            b"2026-00-03T23:58:10Z".to_vec(),
            b"2026-10-00T23:58:10Z".to_vec(),
            b"2026-10-03t23:58:10Z".to_vec(),
            b"2026-10-03 23:58:10Z".to_vec(),
            b"2026/10-03T23:58:10Z".to_vec(),
            b"2026-10-03T23-58:10Z".to_vec(),
        ];
        // every single-byte corruption of a valid stamp
        let base = b"2026-10-03T23:58:10Z";
        for i in 0..19 {
            for c in [
                b'/',
                b':',
                b'0' - 1,
                b'9' + 1,
                b'a',
                b'-',
                b'T',
                0u8,
                0xFF,
                b' ',
            ] {
                let mut v = base.to_vec();
                v[i] = c;
                cases.push(v);
            }
            for c in b'0'..=b'9' {
                let mut v = base.to_vec();
                v[i] = c;
                cases.push(v);
            }
        }
        for k in 1..=16usize {
            for d in [b'0', b'5', b'9'] {
                let mut v = vec![d; k];
                v[0] = b'1';
                cases.push(v.clone());
                v[k - 1] = b'/';
                cases.push(v.clone());
                v[k - 1] = b':';
                cases.push(v);
            }
        }
        for c in cases {
            let mut padded = c.clone();
            padded.extend_from_slice(&[0x5A; 32]);
            assert_eq!(
                unsafe { parse_ts_padded(&padded[..c.len()]) },
                parse_timestamp(&c),
                "{:?}",
                String::from_utf8_lossy(&c)
            );
        }
    }
}
