use std::error::Error;
use std::fmt;

#[derive(Debug, PartialEq)]
pub enum SsacheError {
    NoDataReceived,
    NotEnoughParameters { message: String },
}

impl fmt::Display for SsacheError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoDataReceived => {
                write!(f, "No data received from client")
            }
            Self::NotEnoughParameters { message: _ } => {
                write!(f, "Not enough parameters on command")
            }
        }
    }
}

impl Error for SsacheError {}

#[derive(Debug, PartialEq)]
pub enum SaveError {
    CreatingDump,
    WritingDump,
    SerializingIntoBinary,
}

impl fmt::Display for SaveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CreatingDump => {
                write!(f, "Error creating dump file")
            }
            Self::WritingDump => {
                write!(f, "Error writing the dump to the file")
            }
            Self::SerializingIntoBinary => {
                write!(f, "Error serializing storage into binary format")
            }
        }
    }
}

impl Error for SaveError {}

#[derive(Debug, PartialEq)]
pub enum LoadError {
    DeserializingData,
    ReadingDump,
}

impl fmt::Display for LoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DeserializingData => {
                write!(f, "Error deserializing dump content")
            }
            Self::ReadingDump => {
                write!(f, "Error reading dump file")
            }
        }
    }
}

impl Error for LoadError {}
