use std::collections::{BTreeMap, BTreeSet};

const MAX_DOCUMENT_BYTES: usize = 1024 * 1024;
const MAX_QUERY_BYTES: usize = 4096;
const MAX_TOKENS: usize = 65_536;
const MAX_QUERY_TERMS: usize = 1024;
const MAX_QUERY_DEPTH: usize = 64;

pub const INDEX_FORMAT_VERSION: &str = "1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token {
    pub text: String,
    pub position: usize,
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Query {
    Term { text: String, prefix: bool },
    Not(Box<Query>),
    And(Box<Query>, Box<Query>),
    Or(Box<Query>, Box<Query>),
    Phrase(Box<Query>, Box<Query>),
}

pub fn tokenize(config: &str, input: &str) -> Result<Vec<Token>, String> {
    validate_config(config)?;
    if input.len() > MAX_DOCUMENT_BYTES {
        return Err("document exceeds 1048576 bytes".into());
    }
    let mut output = Vec::new();
    let mut start = None;
    for (offset, ch) in input.char_indices() {
        if ch.is_alphanumeric() || ch == '_' {
            start.get_or_insert(offset);
        } else if let Some(token_start) = start.take() {
            push_token(config, input, token_start, offset, &mut output)?;
        }
    }
    if let Some(token_start) = start {
        push_token(config, input, token_start, input.len(), &mut output)?;
    }
    Ok(output)
}

fn push_token(
    config: &str,
    input: &str,
    start: usize,
    end: usize,
    output: &mut Vec<Token>,
) -> Result<(), String> {
    if output.len() >= MAX_TOKENS {
        return Err("document exceeds 65536 tokens".into());
    }
    output.push(Token {
        text: normalize(config, &input[start..end]),
        position: output.len() + 1,
        start,
        end,
    });
    Ok(())
}

fn validate_config(config: &str) -> Result<(), String> {
    if matches!(config.to_ascii_lowercase().as_str(), "simple" | "english") {
        Ok(())
    } else {
        Err(format!("unsupported text search configuration '{config}'"))
    }
}

fn normalize(config: &str, token: &str) -> String {
    let mut token = token.to_lowercase();
    if config.eq_ignore_ascii_case("english") && token.is_ascii() {
        for suffix in ["ing", "ed", "es", "s"] {
            if token.len() > suffix.len() + 2 && token.ends_with(suffix) {
                token.truncate(token.len() - suffix.len());
                break;
            }
        }
    }
    token
}

pub fn to_tsvector(config: &str, document: &str) -> Result<String, String> {
    let mut terms = BTreeMap::<String, Vec<usize>>::new();
    for token in tokenize(config, document)? {
        terms.entry(token.text).or_default().push(token.position);
    }
    Ok(terms
        .into_iter()
        .map(|(term, positions)| {
            format!(
                "'{}':{}",
                term.replace('\'', "''"),
                positions
                    .into_iter()
                    .map(|position| position.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            )
        })
        .collect::<Vec<_>>()
        .join(" "))
}

pub fn parse_tsquery(config: &str, input: &str) -> Result<Query, String> {
    validate_config(config)?;
    validate_query_input(input)?;
    let tokens = lex_query(config, input)?;
    if tokens.is_empty() {
        return Err("TSQUERY is empty".into());
    }
    if tokens
        .iter()
        .filter(|token| matches!(token, QueryToken::Term(_, _)))
        .count()
        > MAX_QUERY_TERMS
    {
        return Err("TSQUERY exceeds 1024 terms".into());
    }
    let mut parser = QueryParser { tokens, index: 0 };
    let query = parser.parse_or(0)?;
    if parser.index != parser.tokens.len() {
        return Err("TSQUERY contains an unexpected token".into());
    }
    Ok(query)
}

pub fn plainto_tsquery(config: &str, input: &str) -> Result<Query, String> {
    validate_query_input(input)?;
    let tokens = tokenize(config, input)?;
    if tokens.len() > MAX_QUERY_TERMS {
        return Err("TSQUERY exceeds 1024 terms".into());
    }
    terms_to_and(tokens.into_iter().map(|token| token.text))
}

pub fn websearch_to_tsquery(config: &str, input: &str) -> Result<Query, String> {
    validate_config(config)?;
    validate_query_input(input)?;
    let mut groups = Vec::<Vec<Query>>::new();
    let mut current = Vec::new();
    let mut chars = input.char_indices().peekable();
    while let Some((offset, ch)) = chars.next() {
        if ch.is_whitespace() {
            continue;
        }
        if matches!(ch, 'O' | 'o')
            && input[offset..]
                .get(..2)
                .is_some_and(|value| value.eq_ignore_ascii_case("OR"))
            && input[offset + 2..]
                .chars()
                .next()
                .is_none_or(char::is_whitespace)
        {
            chars.next();
            if current.is_empty() {
                return Err("TSQUERY has an empty OR branch".into());
            }
            groups.push(std::mem::take(&mut current));
            continue;
        }
        let negated = ch == '-';
        let first = if negated {
            chars
                .next()
                .ok_or_else(|| "TSQUERY has a dangling '-'".to_string())?
        } else {
            (offset, ch)
        };
        let query = if first.1 == '"' {
            let start = first.0 + first.1.len_utf8();
            let mut end = None;
            for (position, next) in chars.by_ref() {
                if next == '"' {
                    end = Some(position);
                    break;
                }
            }
            let end = end.ok_or_else(|| "TSQUERY has an unterminated quote".to_string())?;
            let terms = tokenize(config, &input[start..end])?;
            terms_to_phrase(terms.into_iter().map(|token| token.text))?
        } else {
            let start = first.0;
            let mut end = input.len();
            while let Some(&(position, next)) = chars.peek() {
                if next.is_whitespace() {
                    end = position;
                    break;
                }
                chars.next();
            }
            let terms = tokenize(config, &input[start..end])?;
            terms_to_and(terms.into_iter().map(|token| token.text))?
        };
        current.push(if negated {
            Query::Not(Box::new(query))
        } else {
            query
        });
    }
    if !current.is_empty() {
        groups.push(current);
    }
    if groups.is_empty() {
        return Err("TSQUERY is empty".into());
    }
    let query = groups
        .into_iter()
        .map(|group| and_queries(group.into_iter()))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .reduce(|left, right| Query::Or(Box::new(left), Box::new(right)))
        .ok_or_else(|| "TSQUERY is empty".to_string())?;
    if query_term_count(&query) > MAX_QUERY_TERMS {
        return Err("TSQUERY exceeds 1024 terms".into());
    }
    Ok(query)
}

fn validate_query_input(input: &str) -> Result<(), String> {
    if input.len() > MAX_QUERY_BYTES {
        Err("TSQUERY exceeds 4096 bytes".into())
    } else {
        Ok(())
    }
}

fn query_term_count(query: &Query) -> usize {
    match query {
        Query::Term { .. } => 1,
        Query::Not(query) => query_term_count(query),
        Query::And(left, right) | Query::Or(left, right) | Query::Phrase(left, right) => {
            query_term_count(left) + query_term_count(right)
        }
    }
}

fn terms_to_and(terms: impl IntoIterator<Item = String>) -> Result<Query, String> {
    and_queries(terms.into_iter().map(|text| Query::Term {
        text,
        prefix: false,
    }))
}

fn terms_to_phrase(terms: impl IntoIterator<Item = String>) -> Result<Query, String> {
    terms
        .into_iter()
        .map(|text| Query::Term {
            text,
            prefix: false,
        })
        .reduce(|left, right| Query::Phrase(Box::new(left), Box::new(right)))
        .ok_or_else(|| "TSQUERY phrase is empty".into())
}

fn and_queries(queries: impl Iterator<Item = Query>) -> Result<Query, String> {
    queries
        .reduce(|left, right| Query::And(Box::new(left), Box::new(right)))
        .ok_or_else(|| "TSQUERY is empty".into())
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum QueryToken {
    Term(String, bool),
    And,
    Or,
    Not,
    Phrase,
    Left,
    Right,
}

fn lex_query(config: &str, input: &str) -> Result<Vec<QueryToken>, String> {
    let mut output = Vec::new();
    let mut index = 0;
    while index < input.len() {
        let ch = input[index..].chars().next().unwrap();
        if ch.is_whitespace() {
            index += ch.len_utf8();
            continue;
        }
        let token = match ch {
            '&' => QueryToken::And,
            '|' => QueryToken::Or,
            '!' => QueryToken::Not,
            '(' => QueryToken::Left,
            ')' => QueryToken::Right,
            '<' if input[index..].starts_with("<->") => {
                index += 2;
                QueryToken::Phrase
            }
            _ if ch.is_alphanumeric() || ch == '_' => {
                let start = index;
                index += ch.len_utf8();
                while index < input.len() {
                    let next = input[index..].chars().next().unwrap();
                    if !(next.is_alphanumeric() || next == '_') {
                        break;
                    }
                    index += next.len_utf8();
                }
                let end = index;
                let prefix = input[index..].starts_with(":*");
                if prefix {
                    index += 2;
                }
                output.push(QueryToken::Term(
                    normalize(config, &input[start..end]),
                    prefix,
                ));
                continue;
            }
            _ => return Err(format!("TSQUERY contains unsupported character '{ch}'")),
        };
        output.push(token);
        index += ch.len_utf8();
    }
    Ok(output)
}

struct QueryParser {
    tokens: Vec<QueryToken>,
    index: usize,
}

impl QueryParser {
    fn parse_or(&mut self, depth: usize) -> Result<Query, String> {
        let mut query = self.parse_and(depth + 1)?;
        while self.take(&QueryToken::Or) {
            query = Query::Or(Box::new(query), Box::new(self.parse_and(depth + 1)?));
        }
        Ok(query)
    }

    fn parse_and(&mut self, depth: usize) -> Result<Query, String> {
        let mut query = self.parse_phrase(depth + 1)?;
        while self.take(&QueryToken::And) {
            query = Query::And(Box::new(query), Box::new(self.parse_phrase(depth + 1)?));
        }
        Ok(query)
    }

    fn parse_phrase(&mut self, depth: usize) -> Result<Query, String> {
        let mut query = self.parse_unary(depth + 1)?;
        while self.take(&QueryToken::Phrase) {
            query = Query::Phrase(Box::new(query), Box::new(self.parse_unary(depth + 1)?));
        }
        Ok(query)
    }

    fn parse_unary(&mut self, depth: usize) -> Result<Query, String> {
        if depth > MAX_QUERY_DEPTH {
            return Err("TSQUERY exceeds nesting depth 64".into());
        }
        if self.take(&QueryToken::Not) {
            return Ok(Query::Not(Box::new(self.parse_unary(depth + 1)?)));
        }
        match self.tokens.get(self.index).cloned() {
            Some(QueryToken::Term(text, prefix)) => {
                self.index += 1;
                Ok(Query::Term { text, prefix })
            }
            Some(QueryToken::Left) => {
                self.index += 1;
                let query = self.parse_or(depth + 1)?;
                if !self.take(&QueryToken::Right) {
                    return Err("TSQUERY is missing ')'".into());
                }
                Ok(query)
            }
            _ => Err("TSQUERY expects a term".into()),
        }
    }

    fn take(&mut self, expected: &QueryToken) -> bool {
        if self.tokens.get(self.index) == Some(expected) {
            self.index += 1;
            true
        } else {
            false
        }
    }
}

pub fn format_query(query: &Query) -> String {
    format_query_precedence(query, 0)
}

fn format_query_precedence(query: &Query, parent: u8) -> String {
    let (precedence, value) = match query {
        Query::Term { text, prefix } => (5, format!("{text}{}", if *prefix { ":*" } else { "" })),
        Query::Not(query) => (4, format!("!{}", format_query_precedence(query, 4))),
        Query::Phrase(left, right) => (
            3,
            format!(
                "{} <-> {}",
                format_query_precedence(left, 3),
                format_query_precedence(right, 3)
            ),
        ),
        Query::And(left, right) => (
            2,
            format!(
                "{} & {}",
                format_query_precedence(left, 2),
                format_query_precedence(right, 2)
            ),
        ),
        Query::Or(left, right) => (
            1,
            format!(
                "{} | {}",
                format_query_precedence(left, 1),
                format_query_precedence(right, 1)
            ),
        ),
    };
    if precedence < parent {
        format!("({value})")
    } else {
        value
    }
}

pub fn matches_query(tokens: &[Token], query: &Query) -> bool {
    !positions(tokens, query).is_empty()
}

fn positions(tokens: &[Token], query: &Query) -> BTreeSet<usize> {
    match query {
        Query::Term { text, prefix } => tokens
            .iter()
            .filter(|token| {
                if *prefix {
                    token.text.starts_with(text)
                } else {
                    token.text == *text
                }
            })
            .map(|token| token.position)
            .collect(),
        Query::Not(query) => positions(tokens, query)
            .is_empty()
            .then_some(0)
            .into_iter()
            .collect(),
        Query::And(left, right) => {
            let left_positions = positions(tokens, left);
            if left_positions.is_empty() || positions(tokens, right).is_empty() {
                BTreeSet::new()
            } else {
                left_positions
            }
        }
        Query::Or(left, right) => positions(tokens, left)
            .union(&positions(tokens, right))
            .copied()
            .collect(),
        Query::Phrase(left, right) => {
            let left = positions(tokens, left);
            positions(tokens, right)
                .into_iter()
                .filter(|position| {
                    position
                        .checked_sub(1)
                        .is_some_and(|previous| left.contains(&previous))
                })
                .collect()
        }
    }
}

pub fn index_terms(query: &Query, output: &mut BTreeSet<String>) -> bool {
    match query {
        Query::Term {
            text,
            prefix: false,
        } => {
            output.insert(text.clone());
            true
        }
        Query::Term { prefix: true, .. } | Query::Not(_) => false,
        Query::And(left, right) | Query::Or(left, right) | Query::Phrase(left, right) => {
            index_terms(left, output) && index_terms(right, output)
        }
    }
}

pub fn rank(tokens: &[Token], query: &Query) -> f64 {
    if !matches_query(tokens, query) || tokens.is_empty() {
        return 0.0;
    }
    let matched = tokens
        .iter()
        .filter(|token| positive_token_match(query, &token.text))
        .count();
    matched as f64 / tokens.len() as f64
}

fn positive_token_match(query: &Query, token: &str) -> bool {
    match query {
        Query::Term { text, prefix } => {
            if *prefix {
                token.starts_with(text)
            } else {
                token == text
            }
        }
        Query::Not(_) => false,
        Query::And(left, right) | Query::Or(left, right) | Query::Phrase(left, right) => {
            positive_token_match(left, token) || positive_token_match(right, token)
        }
    }
}

pub fn headline(config: &str, document: &str, query: &Query) -> Result<String, String> {
    let tokens = tokenize(config, document)?;
    let mut output = String::with_capacity(document.len());
    let mut cursor = 0;
    for token in tokens {
        output.push_str(&document[cursor..token.start]);
        if positive_token_match(query, &token.text) {
            output.push_str("<b>");
            output.push_str(&document[token.start..token.end]);
            output.push_str("</b>");
        } else {
            output.push_str(&document[token.start..token.end]);
        }
        cursor = token.end;
    }
    output.push_str(&document[cursor..]);
    if output.len() > MAX_DOCUMENT_BYTES {
        return Err("headline exceeds 1048576 bytes".into());
    }
    Ok(output)
}

pub fn parse_tsvector(input: &str) -> Result<Vec<Token>, String> {
    if input.len() > MAX_DOCUMENT_BYTES {
        return Err("TSVECTOR exceeds 1048576 bytes".into());
    }
    let mut tokens = Vec::new();
    for entry in input.split_whitespace() {
        let (term, positions) = entry
            .rsplit_once(':')
            .ok_or_else(|| "invalid TSVECTOR entry".to_string())?;
        let term = term
            .strip_prefix('\'')
            .and_then(|value| value.strip_suffix('\''))
            .ok_or_else(|| "invalid TSVECTOR term".to_string())?
            .replace("''", "'");
        for position in positions.split(',') {
            if tokens.len() >= MAX_TOKENS {
                return Err("TSVECTOR exceeds 65536 positions".into());
            }
            let position = position
                .parse::<usize>()
                .map_err(|_| "invalid TSVECTOR position".to_string())?;
            tokens.push(Token {
                text: term.clone(),
                position,
                start: 0,
                end: 0,
            });
        }
    }
    tokens.sort_by_key(|token| token.position);
    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phrase_tracks_the_rightmost_position() {
        let tokens = tokenize("simple", "one two three").unwrap();
        let query = parse_tsquery("simple", "one <-> two <-> three").unwrap();
        assert!(matches_query(&tokens, &query));
    }

    #[test]
    fn vector_round_trip_preserves_rank() {
        let vector = to_tsvector("simple", "the quick brown fox").unwrap();
        let query = plainto_tsquery("simple", "quick fox").unwrap();
        assert_eq!(rank(&parse_tsvector(&vector).unwrap(), &query), 0.5);
    }

    #[test]
    fn public_resource_limits_fail_closed() {
        assert!(tokenize("simple", &"x".repeat(MAX_DOCUMENT_BYTES + 1)).is_err());
        assert!(parse_tsquery("simple", &"x".repeat(MAX_QUERY_BYTES + 1)).is_err());
    }
}
