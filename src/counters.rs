//! Dense counter IDs.
//!
//! The public generator derives every id from a sequential counter:
//! `prefix + base36(scramble(counter, key))` where `scramble` is a bijection
//! on 0..2^40 (multiply by an odd constant, xor the key, xor-shift right 17,
//! multiply by a second odd constant). User ids share one key, event ids
//! another. Every step is invertible, so for ANY key the decoded counter is a
//! bijection of the id string: two ids are equal exactly when their counters
//! are. The true key only decides whether counters come out dense (usable as
//! array indexes); a wrong or missing key costs speed, never correctness.
//!
//! `recover` estimates the keys from a sample of the events head. When it
//! succeeds, canonical ids carry their counter in the u64 key field
//! (`ID_DENSE` marker): dedup probes a direct table instead of hashing, and
//! emit resolves users through a dense table instead of the link map. When it
//! does not converge (non-generator data), everything falls back to the
//! regular byte-key path.

const MASK: u64 = (1 << 40) - 1;
const A: u64 = 0x9E37_79B9_7F4B;
const B: u64 = 0xD6E8_FEB8_6659;

/// Alias slots packed under each user index in the 40-bit counter.
pub const ALIAS_BITS: u32 = 9;
pub const ALIAS_SLOTS: u64 = 1 << ALIAS_BITS;
pub const ALIAS_MASK: u64 = ALIAS_SLOTS - 1;

/// The inverse of an odd number modulo 2^64 (Newton's iteration), masked to
/// 40 bits.
const fn inverse(a: u64) -> u64 {
    let mut x = a; // correct to 3 bits for odd a
    let mut i = 0;
    while i < 6 {
        x = x.wrapping_mul(2u64.wrapping_sub(a.wrapping_mul(x)));
        i += 1;
    }
    x & MASK
}
const A_INV: u64 = inverse(A);
const B_INV: u64 = inverse(B);

#[cfg(test)]
pub fn scramble(value: u64, key: u64) -> u64 {
    let mut m = ((value & MASK).wrapping_mul(A) ^ key) & MASK;
    m ^= m >> 17;
    m.wrapping_mul(B) & MASK
}

/// (counter * A) ^ key for a scrambled value: undoes the last multiply and
/// the xor-shift (17 * 2 < 40 < 17 * 3, so two more shifts cancel exactly).
#[inline(always)]
fn unshuffle(y: u64) -> u64 {
    let x = y.wrapping_mul(B_INV) & MASK;
    x ^ (x >> 17) ^ (x >> 34)
}

/// counter = ((x ^ key) * A^-1) & MASK for x = unshuffle(y). Sampled ids are
/// stored already unshuffled, so this is the only transform the hot paths and
/// the key search need.
#[inline(always)]
pub fn unkey(x: u64, key: u64) -> u64 {
    ((x ^ key).wrapping_mul(A_INV)) & MASK
}

/// counter = ((unshuffle(y) ^ key) * A^-1) & MASK
#[inline(always)]
pub fn unscramble(y: u64, key: u64) -> u64 {
    unkey(unshuffle(y), key)
}

static DIGIT: [u8; 256] = {
    let mut t = [255u8; 256];
    let mut i = 0;
    while i < 10 {
        t[b'0' as usize + i] = i as u8;
        i += 1;
    }
    let mut i = 0;
    while i < 26 {
        t[b'a' as usize + i] = 10 + i as u8;
        i += 1;
    }
    t
};

/// The base-36 value of `prefix + 1..=8 digits in [0-9a-z]` without a leading
/// zero (unless the value is 0), below 2^40. Returns the scrambled value y.
/// The field must be readable with 8 bytes of padding (chunk buffers are).
#[inline]
fn value(f: &[u8], prefix: u8) -> Option<u64> {
    let n = f.len();
    if n < 2 || n > 9 || f[0] != prefix || (n > 2 && f[1] == b'0') {
        return None;
    }
    let m = n - 1;
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use std::arch::x86_64::*;
        let v = _mm_loadl_epi64(f.as_ptr().add(1) as *const __m128i);
        let num = _mm_and_si128(
            _mm_cmpgt_epi8(v, _mm_set1_epi8(b'0' as i8 - 1)),
            _mm_cmplt_epi8(v, _mm_set1_epi8(b'9' as i8 + 1)),
        );
        let lo = _mm_or_si128(v, _mm_set1_epi8(0x20));
        let alpha = _mm_and_si128(
            _mm_cmpgt_epi8(lo, _mm_set1_epi8(b'a' as i8 - 1)),
            _mm_cmplt_epi8(lo, _mm_set1_epi8(b'z' as i8 + 1)),
        );
        let valid = _mm_movemask_epi8(_mm_or_si128(num, alpha)) as u32;
        if valid & ((1u32 << m) - 1) != (1u32 << m) - 1 {
            return None;
        }
        let dnum = _mm_sub_epi8(v, _mm_set1_epi8(b'0' as i8));
        let dalp = _mm_sub_epi8(lo, _mm_set1_epi8((b'a' as i32 - 10) as i8));
        let dig = _mm_or_si128(_mm_and_si128(num, dnum), _mm_and_si128(alpha, dalp));
        // Right-align the m digits (leading lanes become 0) then Horner 36.
        let shuf = SHUF[m - 1];
        let dig = _mm_shuffle_epi8(dig, _mm_loadu_si128(shuf.as_ptr() as *const __m128i));
        // pairs: (d0*36+d1, d2*36+d3, ...), then (p0*36^2+p1, p2*36^2+p3)
        let pairs = _mm_maddubs_epi16(dig, _mm_set1_epi16(0x0124));
        let quads = _mm_madd_epi16(pairs, _mm_set1_epi32(0x0001_0510));
        let mut l = [0i32; 4];
        _mm_storeu_si128(l.as_mut_ptr() as *mut __m128i, quads);
        let y = (l[0] as u64) * 1_679_616 + l[1] as u64;
        return (y <= MASK).then_some(y);
    }
    #[allow(unreachable_code)]
    {
        let mut y = 0u64;
        for &c in &f[1..] {
            let d = DIGIT[c as usize];
            if d == 255 {
                return None;
            }
            y = y * 36 + d as u64;
        }
        (y <= MASK).then_some(y)
    }
}

/// Byte-shuffle tables: dest lane j (< 8) takes source byte j-(8-m) so the
/// m loaded digits right-align; other lanes read 0xFF -> 0.
static SHUF: [[u8; 16]; 8] = {
    let mut t = [[0xFFu8; 16]; 8];
    let mut m = 1usize;
    while m <= 8 {
        let mut j = 0usize;
        while j < 8 {
            if j >= 8 - m {
                t[m - 1][j] = (j - (8 - m)) as u8;
            }
            j += 1;
        }
        m += 1;
    }
    t
};

/// The counter of a canonical id under `key`, if it is below `cap`.
/// `cap == 0` accepts every counter (still 40-bit exact for equality).
#[inline(always)]
pub fn decode(f: &[u8], prefix: u8, key: u64, cap: u64) -> Option<u64> {
    let v = unscramble(value(f, prefix)?, key);
    if v < cap || cap == 0 {
        Some(v)
    } else {
        None
    }
}

/// Re-encode a counter as `prefix + base36(scramble(v, key))` — the exact id
/// string it came from, for the rare paths that still need the bytes. A 40-bit
/// value needs at most 8 base36 digits, so `out` is a fixed stack buffer and
/// the returned length is <= 9.
pub fn encode_into(v: u64, key: u64, prefix: u8, out: &mut [u8; 14]) -> usize {
    out[0] = prefix;
    let mut y = scramble_pub(v, key);
    let mut tmp = [0u8; 9];
    let mut at = tmp.len();
    loop {
        at -= 1;
        tmp[at] = b"0123456789abcdefghijklmnopqrstuvwxyz"[(y % 36) as usize];
        y /= 36;
        if y == 0 {
            break;
        }
    }
    let n = tmp.len() - at;
    out[1..1 + n].copy_from_slice(&tmp[at..]);
    1 + n
}

#[inline]
pub fn scramble_pub(value: u64, key: u64) -> u64 {
    let mut m = ((value & MASK).wrapping_mul(A) ^ key) & MASK;
    m ^= m >> 17;
    m.wrapping_mul(B) & MASK
}

fn unquote(f: &[u8]) -> &[u8] {
    if f.len() >= 2 && f[0] == b'"' && f[f.len() - 1] == b'"' {
        &f[1..f.len() - 1]
    } else {
        f
    }
}

#[derive(Default)]
struct Sample {
    /// unshuffle() of every decodable user id in the sample.
    users: Vec<u64>,
    /// unshuffle() of every decodable event id, with its row's user x.
    events: Vec<(u64, u64)>,
}

fn sample_events(sample: &[u8]) -> Sample {
    let mut s = Sample::default();
    let end = match sample.iter().rposition(|&c| c == b'\n') {
        Some(e) => e,
        None => return s,
    };
    for line in sample[..end].split(|&c| c == b'\n').skip(1) {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        let mut f = line.split(|&c| c == b',');
        let (Some(e), Some(u)) = (f.next(), f.next()) else {
            continue;
        };
        let (e, u) = (unquote(e), unquote(u));
        if let Some(y) = value(u, b'u') {
            let ux = unshuffle(y);
            s.users.push(ux);
            if let Some(ey) = value(e, b'e') {
                s.events.push((unshuffle(ey), ux));
            }
        }
    }
    s
}

#[derive(Clone, Copy, Default)]
pub struct KeySet {
    pub user: u64,
    pub event: u64,
    /// Present when both keys recovered; otherwise decode stays off entirely.
    pub ok: bool,
}

/// Bound on a row-user's index: user ids in rows always come from the
/// ordinary range; kiosk ids only appear inside links.
const USER_BOUND: u64 = 1 << 27;
/// Parse-time acceptance caps: counters beyond them keep the byte-key path,
/// so a sparse or wrongly-keyed id can never blow up the direct table.
pub const EVENT_CAP: u64 = 1 << 28;
pub const USER_CAP: u64 = USER_BOUND << ALIAS_BITS;
/// Bound used to verify event/click counters: covers 268M rows of each kind.
const COUNTER_CAP: u64 = EVENT_CAP;

fn recover_user_key(s: &Sample) -> Option<u64> {
    let mut all = s.users.clone();
    all.sort_unstable();
    let mut runs: Vec<(u32, u64)> = Vec::new();
    for &x in &all {
        match runs.last_mut() {
            Some((c, y)) if *y == x => *c += 1,
            _ => runs.push((1, x)),
        }
    }
    if runs.len() < 20 {
        return None;
    }
    let slack = all.len() / 1000;
    let top = runs.len().min(16);
    runs.select_nth_unstable_by(top - 1, |a, b| b.0.cmp(&a.0));
    runs[..top].sort_unstable_by(|a, b| b.0.cmp(&a.0));
    let anchors: Vec<u64> = runs[..top].iter().map(|&(_, z)| z).collect();
    let ok_user = |v: u64| (v >> ALIAS_BITS) < USER_BOUND;
    let verify = |key: u64, slack: usize| -> bool {
        let mut miss = 0usize;
        let mut last = u64::MAX;
        let mut distinct = 0usize;
        for &(c, x) in &runs {
            let v = unkey(x, key);
            if !ok_user(v) {
                miss += c as usize;
                if miss > slack {
                    return false;
                }
            } else if v >> ALIAS_BITS != last {
                last = v >> ALIAS_BITS;
                distinct += 1;
            }
        }
        distinct >= 20
    };
    // Anchor candidates: each frequent id may be alias 0 of user index i.
    let mut found: Option<u64> = None;
    for z in anchors {
        for i in 0..64u64 {
            let key = (z ^ (i * ALIAS_SLOTS).wrapping_mul(A)) & MASK;
            if verify(key, slack) {
                if found.is_some_and(|f| f != key) {
                    return None; // ambiguous: give up rather than guess
                }
                found = Some(key);
            }
        }
    }
    found
}



fn recover_event_key(s: &Sample, ukey: u64) -> Option<u64> {
    // Rows of the smallest user index have the smallest counters: first_event
    // is allocated in index order, so anchor guesses search only that user's
    // own range.
    let mut rows: Vec<u64> = Vec::new();
    let mut u_min = u64::MAX;
    for &(x, ux) in &s.events {
        let v = unkey(ux, ukey);
        let u = v >> ALIAS_BITS;
        if u < u_min {
            u_min = u;
            rows.clear();
            rows.push(x);
        } else if u == u_min {
            rows.push(x);
        }
    }
    if rows.is_empty() {
        return None;
    }
    let mut distinct: Vec<u64> = s.events.iter().map(|e| e.0).collect();
    distinct.sort_unstable();
    distinct.dedup();
    if distinct.len() < 64 {
        return None;
    }
    let verify = |key: u64| -> bool {
        let mut good = 0usize;
        for &x in &distinct {
            good += (unkey(x, key) < COUNTER_CAP) as usize;
        }
        good * 20 >= distinct.len() * 19
    };
    let (x2, x3) = (*distinct.first().unwrap(), *distinct.last().unwrap());
    for a in 0..rows.len().min(4) {
        let z = rows[a];
        // counter guess v -> key = z ^ v*A; two other ids must land below cap.
        let w1 = z ^ x2;
        let w2 = z ^ x3;
        let limit = 1u64 << 24;
        let mut x = 0u64; // v * A mod 2^40
        let mut v = 0u64;
        while v < limit {
            if (w1 ^ x).wrapping_mul(A_INV) & MASK < COUNTER_CAP
                && (w2 ^ x).wrapping_mul(A_INV) & MASK < COUNTER_CAP
            {
                let key = (z ^ x) & MASK;
                if verify(key) {
                    return Some(key);
                }
            }
            x = x.wrapping_add(A) & MASK;
            v += 1;
        }
    }
    None
}

/// Estimates the generator keys from the head of the events input. The whole
/// dense path is conditional on this converging; `ok == false` keeps the
/// byte-key pipeline.
pub fn recover(events_heads: &[Vec<u8>]) -> Option<KeySet> {
    let mut s = Sample::default();
    for head in events_heads {
        let ss = sample_events(head);
        s.users.extend(ss.users);
        s.events.extend(ss.events);
    }
    if s.users.len() < 256 || s.events.len() < 256 {
        return None;
    }
    let user = recover_user_key(&s)?;
    let event = recover_event_key(&s, user)?;
    Some(KeySet {
        user,
        event,
        ok: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(y: u64, prefix: u8) -> Vec<u8> {
        let mut d = vec![prefix];
        let mut r = y;
        let mut b = Vec::new();
        loop {
            b.push(b"0123456789abcdefghijklmnopqrstuvwxyz"[(r % 36) as usize]);
            r /= 36;
            if r == 0 {
                break;
            }
        }
        b.reverse();
        d.extend_from_slice(&b);
        d
    }

    #[test]
    fn inverts() {
        assert_eq!(A.wrapping_mul(A_INV) & MASK, 1);
        assert_eq!(B.wrapping_mul(B_INV) & MASK, 1);
        for key in [0, 1, 0xf2_4638_c44b, MASK] {
            for v in [0, 1, 2, 12345, 1 << 39, MASK] {
                assert_eq!(unscramble(scramble(v, key), key), v & MASK);
            }
        }
        assert_eq!(value(b"e0", b'e'), Some(0));
        assert_eq!(value(b"e00", b'e'), None);
        assert_eq!(value(b"e10", b'e'), Some(36));
        assert_eq!(value(b"x1", b'e'), None);
    }

    #[test]
    fn recovers() {
        let ukey = 0x1234_5678_9abc & MASK;
        let ekey = 0x0f0e_0d0c_0b0a & MASK;
        let mut s = String::from("event_id,user_id,ts,ingest_ts,kind,product,price\n");
        // User 0's ids and events, then spread users/events.
        let mut v = 0x9E37u64;
        let mut rnd = move || {
            v = v.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            v >> 24
        };
        for i in 0..800u64 {
            let u = if i < 40 { 0 } else { rnd() % 300 };
            let a = if i < 40 { 0 } else { rnd() % 3 };
            let ev = if i < 40 { i } else { rnd() % 2_000_000 };
            s.push_str(&String::from_utf8(id(scramble(ev, ekey), b'e')).unwrap());
            s.push(',');
            s.push_str(&String::from_utf8(id(scramble(u * 512 + a, ukey), b'u')).unwrap());
            s.push_str(",t,t,view,p1,\n");
        }
        let keys = recover(&[s.into_bytes()]).expect("recover");
        assert_eq!(keys.user, ukey);
        assert_eq!(keys.event, ekey);
    }
}
