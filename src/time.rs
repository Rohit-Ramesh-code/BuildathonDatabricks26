//! Timestamp parsing for the three forms in rules.md.
//! Digits are ASCII only. A value that does not fit in i64 is outside any window.

const DAY_MS: i64 = 86_400_000;

#[inline]
pub fn parse_ts(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 20 && s[19] == b'Z' {
        return parse_datetime_prefix(s);
    }
    if n == 13 {
        return parse_digits_fixed(s);
    }
    if n == 25 && (s[19] == b'+' || s[19] == b'-') && s[22] == b':' {
        let local = parse_datetime_prefix(s)?;
        let hh = dig2(s[20], s[21])?;
        let mm = dig2(s[23], s[24])?;
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

#[inline]
fn parse_datetime_prefix(s: &[u8]) -> Option<i64> {
    if s.len() < 19 {
        return None;
    }
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let year = dig4(s[0], s[1], s[2], s[3])?;
    let month = dig2(s[5], s[6])?;
    let day = dig2(s[8], s[9])?;
    let hour = dig2(s[11], s[12])?;
    let minute = dig2(s[14], s[15])?;
    let second = dig2(s[17], s[18])?;
    if !(1..=12).contains(&month) || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    if !valid_day(year, month, day) {
        return None;
    }
    let days = days_from_civil(year, month, day);
    Some(days * DAY_MS + ((hour * 60 + minute) * 60 + second) * 1_000)
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

fn dig2(a: u8, b: u8) -> Option<i64> {
    if a.is_ascii_digit() && b.is_ascii_digit() {
        Some(((a - b'0') * 10 + (b - b'0')) as i64)
    } else {
        None
    }
}

fn dig4(a: u8, b: u8, c: u8, d: u8) -> Option<i64> {
    if a.is_ascii_digit() && b.is_ascii_digit() && c.is_ascii_digit() && d.is_ascii_digit() {
        Some(
            ((a - b'0') as i64) * 1000
                + ((b - b'0') as i64) * 100
                + ((c - b'0') as i64) * 10
                + (d - b'0') as i64,
        )
    } else {
        None
    }
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
