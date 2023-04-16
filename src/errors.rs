use std::error::Error;
use std::fmt;

#[derive(Debug, PartialEq)]
pub enum SsacheErrorKind {
    NoDataReceived,
    NotEnoughParameters { message: String },
}

impl fmt::Display for SsacheErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Ssache error")
    }
}

impl Error for SsacheErrorKind {}

#[derive(Debug, PartialEq)]
pub enum SaveErrorKind {
    UnableToCreateDump,
    UnableToWriteToDump,
    UnableToSerializeIntoBinary,
}

impl fmt::Display for SaveErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Error on save")
    }
}

impl Error for SaveErrorKind {}

#[derive(Debug, PartialEq)]
pub enum LoadErrorKind {
    UnableToDeserializaData,
    UnableToReadDump,
}

impl fmt::Display for LoadErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Error on load")
    }
}

impl Error for LoadErrorKind {}
