use super::RunDslError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Token {
    pub(crate) kind: TokenKind,
    pub(crate) start: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TokenKind {
    Word(String),
    Quoted(String),
    Backtick(String),
    Comma,
    Equals,
    LeftParen,
    RightParen,
}

impl Token {
    pub(crate) fn value(&self) -> Option<&str> {
        match &self.kind {
            TokenKind::Word(value) | TokenKind::Quoted(value) | TokenKind::Backtick(value) => {
                Some(value)
            }
            _ => None,
        }
    }

    pub(crate) fn is_word(&self, expected: &str) -> bool {
        matches!(&self.kind, TokenKind::Word(value) if value.eq_ignore_ascii_case(expected))
    }
}

pub(crate) fn lex(input: &str) -> Result<Vec<Token>, RunDslError> {
    let mut tokens = Vec::new();
    let mut index = 0;
    while index < input.len() {
        let character = next_character(input, index);
        if character.is_whitespace() {
            index += character.len_utf8();
            continue;
        }
        let start = index;
        let kind = match character {
            ',' => {
                index += 1;
                TokenKind::Comma
            }
            '=' => {
                index += 1;
                TokenKind::Equals
            }
            '(' => {
                index += 1;
                TokenKind::LeftParen
            }
            ')' => {
                index += 1;
                TokenKind::RightParen
            }
            '\'' => {
                let (value, next) = quoted_value(input, start, '\'')?;
                index = next;
                TokenKind::Quoted(value)
            }
            '`' => {
                let (value, next) = quoted_value(input, start, '`')?;
                index = next;
                TokenKind::Backtick(value)
            }
            _ => {
                while index < input.len() {
                    let next = next_character(input, index);
                    if next.is_whitespace() || matches!(next, ',' | '=' | '(' | ')' | '\'' | '`') {
                        break;
                    }
                    index += next.len_utf8();
                }
                TokenKind::Word(input[start..index].to_owned())
            }
        };
        tokens.push(Token { kind, start });
    }
    Ok(tokens)
}

fn quoted_value(
    input: &str,
    start: usize,
    delimiter: char,
) -> Result<(String, usize), RunDslError> {
    let mut value = String::new();
    let mut index = start + delimiter.len_utf8();
    while index < input.len() {
        let character = next_character(input, index);
        if character == delimiter {
            return Ok((value, index + delimiter.len_utf8()));
        }
        if delimiter == '\'' && character == '\\' {
            let escaped_index = index + character.len_utf8();
            if escaped_index >= input.len() {
                break;
            }
            let escaped = next_character(input, escaped_index);
            if escaped == '\'' || escaped == '\\' {
                value.push(escaped);
                index = escaped_index + escaped.len_utf8();
                continue;
            }
        }
        value.push(character);
        index += character.len_utf8();
    }
    Err(RunDslError::new(
        "INVALID_DSL",
        "task",
        start,
        format!("unterminated {delimiter} quoted value"),
        format!("close the value with {delimiter}"),
    ))
}

fn next_character(input: &str, index: usize) -> char {
    input[index..]
        .chars()
        .next()
        .expect("index is inside the input")
}
