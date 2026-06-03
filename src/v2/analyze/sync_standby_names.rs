//! Minimal parser for postgres `synchronous_standby_names`.
//!
//! Grammar (subset):
//!   ssn := ('ANY' | 'FIRST') num '(' name (',' name)* ')'
//!        | name (',' name)*       -- legacy form, treated as FIRST 1
//!        | ''                      -- no sync standbys.
//!
//! Postgres also supports application-name quoting and full SQL identifier
//! rules; we accept a pragmatic subset matching repmgr-generated configs.

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Method {
    Any,
    First,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Quorum {
    pub method: Method,
    pub count: u32,
    pub members: Vec<String>,
}

pub fn parse(input: &str) -> Option<Quorum> {
    let s = input.trim();
    if s.is_empty() {
        return None;
    }

    // Strip optional method prefix.
    let (method, rest) =
        if let Some(rest) = s.strip_prefix("ANY ").or_else(|| s.strip_prefix("any ")) {
            (Method::Any, rest.trim_start())
        } else if let Some(rest) = s
            .strip_prefix("FIRST ")
            .or_else(|| s.strip_prefix("first "))
        {
            (Method::First, rest.trim_start())
        } else {
            // Legacy form: list of names = FIRST 1
            return Some(Quorum {
                method: Method::First,
                count: 1,
                members: split_members(s)?,
            });
        };

    // Expect `<count> ( <members> )`.
    let (count_str, rest) = rest.split_once('(')?;
    let count: u32 = count_str.trim().parse().ok()?;
    let members_str = rest.trim().strip_suffix(')')?;
    Some(Quorum {
        method,
        count,
        members: split_members(members_str)?,
    })
}

fn split_members(s: &str) -> Option<Vec<String>> {
    let members: Vec<String> = s
        .split(',')
        .map(|m| m.trim().trim_matches('"').to_owned())
        .filter(|m| !m.is_empty())
        .collect();
    if members.is_empty() {
        None
    } else {
        Some(members)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn parses_any_1_a_b() {
        let q = parse("ANY 1 ( A, B )").unwrap();
        assert_eq!(q.method, Method::Any);
        assert_eq!(q.count, 1);
        assert_eq!(q.members, vec!["A".to_owned(), "B".to_owned()]);
    }

    #[test]
    fn parses_first_2_with_quotes() {
        let q = parse("FIRST 2 (\"db001\", \"db002\")").unwrap();
        assert_eq!(q.count, 2);
        assert_eq!(q.members, vec!["db001".to_owned(), "db002".to_owned()]);
    }

    #[test]
    fn parses_legacy_form_as_first_1() {
        let q = parse("db001, db002").unwrap();
        assert_eq!(q.method, Method::First);
        assert_eq!(q.count, 1);
    }

    #[test]
    fn empty_returns_none() {
        assert_eq!(parse(""), None);
        assert_eq!(parse("  "), None);
    }
}
