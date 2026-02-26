/// Shared glob-style pattern matching for Redis compatibility.
///
/// Supports the full Redis glob pattern syntax:
/// - `*` matches any sequence of characters (including empty)
/// - `?` matches any single character
/// - `[abc]` matches any character in the brackets
/// - `[^abc]` or `[!abc]` matches any character NOT in the brackets
/// - `[a-z]` matches any character in the range
/// - `\x` matches the literal character x (escape)

/// Match a glob pattern against a string.
pub fn glob_match(pattern: &str, text: &str) -> bool {
    glob_match_bytes(pattern.as_bytes(), text.as_bytes())
}

/// Match a glob pattern against byte slices.
pub fn glob_match_bytes(pattern: &[u8], text: &[u8]) -> bool {
    let mut pi = 0;
    let mut ti = 0;
    let mut star_pi = None;
    let mut star_ti = None;

    while ti < text.len() {
        if pi < pattern.len() {
            match pattern[pi] {
                b'*' => {
                    star_pi = Some(pi);
                    star_ti = Some(ti);
                    pi += 1;
                    continue;
                }
                b'?' => {
                    pi += 1;
                    ti += 1;
                    continue;
                }
                b'[' => {
                    if let Some((matched, new_pi)) = match_char_class(pattern, pi, text[ti]) {
                        if matched {
                            pi = new_pi;
                            ti += 1;
                            continue;
                        }
                    }
                }
                b'\\' if pi + 1 < pattern.len() => {
                    if pattern[pi + 1] == text[ti] {
                        pi += 2;
                        ti += 1;
                        continue;
                    }
                }
                c if c == text[ti] => {
                    pi += 1;
                    ti += 1;
                    continue;
                }
                _ => {}
            }
        }

        if let (Some(sp), Some(st)) = (star_pi, star_ti) {
            pi = sp + 1;
            star_ti = Some(st + 1);
            ti = st + 1;
        } else {
            return false;
        }
    }

    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }

    pi == pattern.len()
}

fn match_char_class(pattern: &[u8], start: usize, ch: u8) -> Option<(bool, usize)> {
    let mut i = start + 1;
    let mut negated = false;
    let mut matched = false;

    if i < pattern.len() && (pattern[i] == b'^' || pattern[i] == b'!') {
        negated = true;
        i += 1;
    }

    let mut first = true;
    while i < pattern.len() {
        if pattern[i] == b']' && !first {
            return Some((if negated { !matched } else { matched }, i + 1));
        }

        first = false;

        // Handle range (e.g., a-z)
        if i + 2 < pattern.len() && pattern[i + 1] == b'-' && pattern[i + 2] != b']' {
            let start_ch = pattern[i];
            let end_ch = pattern[i + 2];
            if ch >= start_ch && ch <= end_ch {
                matched = true;
            }
            i += 3;
        } else {
            if pattern[i] == ch {
                matched = true;
            }
            i += 1;
        }
    }

    None // Malformed pattern (unclosed bracket)
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Star wildcard ---

    #[test]
    fn test_star_matches_anything() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("*", ""));
        assert!(glob_match("*", "a"));
    }

    #[test]
    fn test_star_prefix() {
        assert!(glob_match("hello*", "hello world"));
        assert!(glob_match("hello*", "hello"));
        assert!(!glob_match("hello*", "hell"));
    }

    #[test]
    fn test_star_suffix() {
        assert!(glob_match("*world", "hello world"));
        assert!(glob_match("*world", "world"));
        assert!(!glob_match("*world", "worlds"));
    }

    #[test]
    fn test_star_middle() {
        assert!(glob_match("h*d", "helloworld"));
        assert!(glob_match("h*d", "hd"));
        assert!(!glob_match("h*d", "hello"));
    }

    #[test]
    fn test_multiple_stars() {
        assert!(glob_match("*:*", "news:sports"));
        assert!(glob_match("user:*:messages", "user:123:messages"));
        assert!(glob_match("*a*b*", "xaybz"));
        assert!(!glob_match("*a*b*", "xyz"));
    }

    #[test]
    fn test_star_redis_patterns() {
        assert!(glob_match("news:*", "news:sports"));
        assert!(glob_match("news:*", "news:"));
        assert!(!glob_match("news:*", "weather:sports"));
        assert!(glob_match("*:sports", "news:sports"));
        assert!(glob_match("user:*", "user:123"));
        assert!(!glob_match("user:*", "admin:123"));
        assert!(glob_match("cache:*:data", "cache:user:data"));
        assert!(glob_match("prefix:*", "prefix:"));
    }

    // --- Question mark ---

    #[test]
    fn test_question_mark() {
        assert!(glob_match("h?llo", "hello"));
        assert!(glob_match("h?llo", "hallo"));
        assert!(!glob_match("h?llo", "hllo"));
        assert!(!glob_match("h?llo", "heello"));
    }

    #[test]
    fn test_question_multiple() {
        assert!(glob_match("??", "ab"));
        assert!(!glob_match("??", "a"));
        assert!(!glob_match("??", "abc"));
    }

    // --- Character classes ---

    #[test]
    fn test_char_class_basic() {
        assert!(glob_match("h[ae]llo", "hello"));
        assert!(glob_match("h[ae]llo", "hallo"));
        assert!(!glob_match("h[ae]llo", "hillo"));
    }

    #[test]
    fn test_char_class_negated() {
        assert!(!glob_match("h[^ae]llo", "hello"));
        assert!(!glob_match("h[!ae]llo", "hello"));
        assert!(glob_match("h[^ae]llo", "hillo"));
        assert!(glob_match("h[!ae]llo", "hillo"));
    }

    #[test]
    fn test_char_class_range() {
        assert!(glob_match("user:[0-9]", "user:5"));
        assert!(!glob_match("user:[0-9]", "user:a"));
        assert!(glob_match("[a-z]", "m"));
        assert!(!glob_match("[a-z]", "M"));
        assert!(glob_match("[A-Z]", "M"));
    }

    #[test]
    fn test_char_class_negated_range() {
        assert!(!glob_match("[^0-9]", "5"));
        assert!(glob_match("[^0-9]", "a"));
    }

    // --- Escape sequences ---

    #[test]
    fn test_escape_star() {
        assert!(glob_match("hello\\*world", "hello*world"));
        assert!(!glob_match("hello\\*world", "helloXworld"));
    }

    #[test]
    fn test_escape_question() {
        assert!(glob_match("hello\\?", "hello?"));
        assert!(!glob_match("hello\\?", "helloX"));
    }

    #[test]
    fn test_escape_bracket() {
        assert!(glob_match("hello\\[world", "hello[world"));
        assert!(!glob_match("hello\\[world", "helloXworld"));
    }

    // --- Edge cases ---

    #[test]
    fn test_empty_pattern_empty_text() {
        assert!(glob_match("", ""));
    }

    #[test]
    fn test_empty_pattern_nonempty_text() {
        assert!(!glob_match("", "hello"));
    }

    #[test]
    fn test_exact_match() {
        assert!(glob_match("hello", "hello"));
        assert!(!glob_match("hello", "world"));
    }

    #[test]
    fn test_consecutive_stars() {
        assert!(glob_match("**", "anything"));
        assert!(glob_match("a**b", "ab"));
        assert!(glob_match("a**b", "aXb"));
    }

    #[test]
    fn test_only_stars() {
        assert!(glob_match("***", ""));
        assert!(glob_match("***", "xyz"));
    }

    // --- Byte-level tests ---

    #[test]
    fn test_bytes_star() {
        assert!(glob_match_bytes(b"*", b"anything"));
        assert!(glob_match_bytes(b"*", b""));
    }

    #[test]
    fn test_bytes_pattern() {
        assert!(glob_match_bytes(b"news:*", b"news:sports"));
        assert!(!glob_match_bytes(b"news:*", b"weather:sports"));
    }

    #[test]
    fn test_bytes_char_class() {
        assert!(glob_match_bytes(b"h[ae]llo", b"hello"));
        assert!(!glob_match_bytes(b"h[ae]llo", b"hillo"));
    }

    #[test]
    fn test_bytes_escape() {
        assert!(glob_match_bytes(b"hello\\*world", b"hello*world"));
        assert!(!glob_match_bytes(b"hello\\*world", b"helloXworld"));
    }

    #[test]
    fn test_sentinel_patterns() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("my*", "mymaster"));
        assert!(glob_match("my*", "myother"));
        assert!(!glob_match("my*", "other"));
        assert!(glob_match("?y*", "mymaster"));
        assert!(glob_match("mymaster", "mymaster"));
        assert!(!glob_match("mymaster", "myother"));
    }
}
