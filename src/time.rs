//! Timestamp parsing for the three forms in rules.md.
//! Digits are ASCII only. A value that does not fit in i64 is outside any window.

const DAY_MS: i64 = 86_400_000;

/// Calendar days for dates that repeat across the file. A miss checks the calendar.
pub struct DateCache {
    keys: [u32; 64],
    days: [i64; 64],
}

impl DateCache {
    pub const fn new() -> Self {
        Self { keys: [0; 64], days: [0; 64] }
    }

    #[inline(always)]
    pub fn day_ms(&mut self, year: i64, month: i64, day: i64) -> Option<i64> {
        if !(1..=12).contains(&month) {
            return None;
        }
        let key = (year as u32).wrapping_mul(512) + (month as u32) * 32 + day as u32 + 1;
        let slot = (key as usize) & 63;
        if self.keys[slot] == key {
            return Some(self.days[slot]);
        }
        if !valid_day(year, month, day) {
            return None;
        }
        let days = days_from_civil(year, month, day);
        self.keys[slot] = key;
        self.days[slot] = days;
        Some(days)
    }
}

#[inline]
pub fn parse_ts(s: &[u8]) -> Option<i64> {
    parse_ts_cached(s, &mut DateCache::new())
}

#[inline(always)]
pub fn parse_ts_cached(s: &[u8], dates: &mut DateCache) -> Option<i64> {
    let n = s.len();
    if n == 20 && s[19] == b'Z' {
        return parse_f1(s, dates);
    }
    if n == 13 {
        return parse_digits_fixed(s);
    }
    if n == 25 && (s[19] == b'+' || s[19] == b'-') && s[22] == b':' {
        let local = parse_f1(s, dates)?;
        let hh = dig2_raw(s[20], s[21])?;
        let mm = dig2_raw(s[23], s[24])?;
        if hh > 23 || mm > 59 {
            return None;
        }
        let mut off = (hh * 60 + mm) * 60_000;
        if s[19] == b'-' {
            off = -off;
        }
        return Some(local - off);
    }
    parse_f3(s)
}

#[inline(always)]
fn parse_f1(s: &[u8], dates: &mut DateCache) -> Option<i64> {
    if s.len() < 19 || s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let year = dig4_fast(s)?;
    let month = dig2_raw(s[5], s[6])?;
    let day = dig2_raw(s[8], s[9])?;
    let hour = dig2_raw(s[11], s[12])?;
    let minute = dig2_raw(s[14], s[15])?;
    let second = dig2_raw(s[17], s[18])?;
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let days = dates.day_ms(year, month, day)?;
    Some(days * DAY_MS + ((hour * 60 + minute) * 60 + second) * 1_000)
}

#[inline(always)]
fn dig2_raw(a: u8, b: u8) -> Option<i64> {
    let da = a.wrapping_sub(b'0');
    let db = b.wrapping_sub(b'0');
    if da > 9 || db > 9 {
        None
    } else {
        Some(i64::from(da) * 10 + i64::from(db))
    }
}

#[inline(always)]
fn dig4_fast(s: &[u8]) -> Option<i64> {
    let a = s[0].wrapping_sub(b'0');
    let b = s[1].wrapping_sub(b'0');
    let c = s[2].wrapping_sub(b'0');
    let d = s[3].wrapping_sub(b'0');
    if a > 9 || b > 9 || c > 9 || d > 9 {
        None
    } else {
        Some(i64::from(a) * 1000 + i64::from(b) * 100 + i64::from(c) * 10 + i64::from(d))
    }
}

fn parse_digits_fixed(s: &[u8]) -> Option<i64> {
    let mut value: i64 = 0;
    for &c in s {
        if !c.is_ascii_digit() {
            return None;
        }
        value = value * 10 + (c - b'0') as i64;
    }
    Some(value)
}

fn parse_f3(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    let mut i = 0;
    while i < s.len() {
        if !s[i].is_ascii_digit() {
            return None;
        }
        i += 1;
    }
    i = 0;
    while i < s.len() && s[i] == b'0' {
        i += 1;
    }
    let digits = &s[i..];
    if digits.is_empty() {
        return Some(0);
    }
    if digits.len() > 19 || (digits.len() == 19 && digits > b"9223372036854775807".as_slice()) {
        return None;
    }
    let mut value: i64 = 0;
    for &c in digits {
        value = value * 10 + (c - b'0') as i64;
    }
    Some(value)
}

fn is_leap(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

fn valid_day(year: i64, month: i64, day: i64) -> bool {
    if day < 1 {
        return false;
    }
    let max = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap(year) {
                29
            } else {
                28
            }
        }
        _ => return false,
    };
    day <= max
}

/// Howard Hinnant's `days_from_civil`. Day 0 is 1970-01-01.
pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let day_of_year = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    era * 146_097 + year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_and_offsets() {
        assert_eq!(parse_ts(b"1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(parse_ts(b"1970-01-02T00:00:00Z"), Some(86_400_000));
        assert_eq!(parse_ts(b"1970-01-01T00:00:01Z"), Some(1_000));
        assert_eq!(
            parse_ts(b"2026-10-03T23:58:10Z"),
            parse_ts(b"2026-10-03T19:58:10-04:00")
        );
        assert_eq!(
            parse_ts(b"2026-10-04T01:30:00+02:00"),
            parse_ts(b"2026-10-03T23:30:00Z")
        );
        assert_eq!(
            parse_ts(b"2026-10-03T12:00:00-00:00"),
            parse_ts(b"2026-10-03T12:00:00Z")
        );
        assert_eq!(
            parse_ts(b"2026-10-03T00:00:00Z"),
            parse_ts(b"2026-10-03T23:59:00+23:59")
        );
    }

    #[test]
    fn rejects_bad_forms() {
        assert_eq!(parse_ts(b""), None);
        assert_eq!(parse_ts(b"2026-10-03T23:58:10z"), None);
        assert_eq!(parse_ts(b"2026-10-03t23:58:10Z"), None);
        assert_eq!(parse_ts(b"2026-10-03 23:58:10Z"), None);
        assert_eq!(parse_ts(b"2026-10-03T23:58:10"), None);
        assert_eq!(parse_ts(b"2026-10-03T23:58:10.000Z"), None);
        assert_eq!(parse_ts(b"1791071890000.0"), None);
        assert_eq!(parse_ts(b"1.79e12"), None);
        assert_eq!(parse_ts(b"-5"), None);
        assert_eq!(parse_ts(b"2026-13-03T00:00:00Z"), None);
        assert_eq!(parse_ts(b"2026-02-30T00:00:00Z"), None);
        assert_eq!(parse_ts(b"2027-02-29T00:00:00Z"), None);
        assert_eq!(parse_ts(b"2026-10-03T24:00:00Z"), None);
        assert_eq!(parse_ts(b"2026-10-03T23:58:60Z"), None);
        assert_eq!(parse_ts(b"2026-10-03T00:00:00+24:00"), None);
        assert_eq!(parse_ts(b"2026-10-03T00:00:00+05:60"), None);
        assert_eq!(parse_ts(b"2026-10-03T00:00:00+0200"), None);
        assert_eq!(parse_ts("٢٠٢٦".as_bytes()), None);
        assert!(parse_ts(b"2028-02-29T00:00:00Z").is_some());
    }

    #[test]
    fn f3_leading_zeros_and_range() {
        assert_eq!(parse_ts(b"0001791071890000"), parse_ts(b"1791071890000"));
        assert_eq!(parse_ts(b"86400000"), Some(86_400_000));
        assert_eq!(parse_ts(b"000"), Some(0));
        assert_eq!(parse_ts(b"9223372036854775808"), None);
        assert_eq!(parse_ts(b"00000000000000000000001"), Some(1));
        assert_eq!(parse_ts(b"0009223372036854775808"), None);
        assert_eq!(days_from_civil(1970, 1, 1), 0);
    }
}
