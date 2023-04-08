use std::error::Error;
use std::fmt;

#[derive(Debug, PartialEq)]
pub enum SsacheErrorKind {
    NoDataReceived,
    NotEnoughParameters { message: String },
}

impl fmt::Display for SsacheErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SuperError is here!")
    }
}

impl Error for SsacheErrorKind {}
