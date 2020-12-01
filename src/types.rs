
use ::utils;

use std::str;
use anyhow::{anyhow};
use atoi::atoi;

use bytes::{Buf, Bytes};
use std::convert::TryInto;
use std::fmt;
use std::io::Cursor;
use std::num::TryFromIntError;
use std::string::FromUtf8Error;


use serde::{Serialize, Deserialize};

pub const SIMPLESTRING_BYTE: u8 = b'+';
pub const ERROR_BYTE: u8        = b'-';
pub const INTEGER_BYTE: u8      = b':';
pub const BULKSTRING_BYTE: u8   = b'$';
pub const ARRAY_BYTE: u8        = b'*';

/// A cluster redirection message.
///
/// <https://redis.io/topics/cluster-spec#redirection-and-resharding>
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Redirection {
  Moved {
    slot: u16,
    host: String,
    port: u16
  },
  Ask {
    slot: u16,
    host: String,
    port: u16
  }
}

/// An enum representing a Frame of data. Frames are recursively defined to account for arrays.
#[derive(Eq, PartialEq, Clone, Hash, Debug , Serialize, Deserialize)]
pub enum Frame {
  SimpleString(String),
  Error(String),
  Integer(i64),
  #[serde(with = "my_bytes")]
  BulkString(Bytes),
  Array(Vec<Frame>),
  Moved(String),
  Ask(String),
  Null
}

mod my_bytes {
  use bytes::{Bytes};
  use serde::{Deserialize, Deserializer, Serializer};

  pub fn serialize<S>(val: &Bytes, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
  {
    serializer.serialize_bytes(val)
  }

  pub fn deserialize<'de, D>(deserializer: D) -> Result<Bytes, D::Error>
    where
        D: Deserializer<'de>,
  {
    let val: Vec<u8> = Deserialize::deserialize(deserializer)?;
    Ok(Bytes::from(val))
  }
}

impl Frame {
  /// Checks if an entire message can be decoded from `src`
  pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
    match get_u8(src)? {
      b'+' => {
        get_line(src)?;
        Ok(())
      }
      b'-' => {
        get_line(src)?;
        Ok(())
      }
      b':' => {
        let _ = get_decimal(src)?;
        Ok(())
      }
      b'$' => {
        if b'-' == peek_u8(src)? {
          // Skip '-1\r\n'
          skip(src, 4)
        } else {
          // Read the bulk string
          let len: usize = get_decimal(src)?.try_into()?;

          // skip that number of bytes + 2 (\r\n).
          skip(src, len + 2)
        }
      }
      b'*' => {
        let len = get_decimal(src)?;

        for _ in 0..len {
          Frame::check(src)?;
        }

        Ok(())
      }
      actual => Err(format!("protocol error; invalid frame type byte `{}`", actual).into()),
    }
  }

  /// The message has already been validated with `check`.
  pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
    match get_u8(src)? {
      b'+' => {
        // Read the line and convert it to `Vec<u8>`
        let line = get_line(src)?.to_vec();

        // Convert the line to a String
        let string = String::from_utf8(line)?;

        Ok(Frame::SimpleString(string))
      }
      b'-' => {
        // Read the line and convert it to `Vec<u8>`
        let line = get_line(src)?.to_vec();

        // Convert the line to a String
        let string = String::from_utf8(line)?;

        return if let Ok(r) =  utils::string_to_redirection(&string) {
          Ok(Frame::from(r))
        } else {
          Ok(Frame::Error(string))
        }

      }
      b':' => {
        let len = get_decimal(src)?;
        Ok(Frame::Integer(len))
      }
      b'$' => {
        if b'-' == peek_u8(src)? {
          let line = get_line(src)?;

          if line != b"-1" {
            return Err("protocol error; invalid frame format".into());
          }

          Ok(Frame::Null)
        } else {
          // Read the bulk string
          let len = get_decimal(src)?.try_into()?;
          let n = len + 2;

          if src.remaining() < n {
            println!("{}", src.remaining());
            return Err(Error::Incomplete);
          }

          let data = Bytes::copy_from_slice(&src.bytes()[..len]);

          // skip that number of bytes + 2 (\r\n).
          skip(src, n)?;

          Ok(Frame::BulkString(data))
        }
      }
      b'*' => {
        let len = get_decimal(src)?.try_into()?;
        let mut out = Vec::with_capacity(len);

        for _ in 0..len {
          out.push(Frame::parse(src)?);
        }

        Ok(Frame::Array(out))
      }
      _ => unimplemented!(),
    }
  }

  pub fn as_str(&self) -> Option<&str> {
    match *self {
      Frame::BulkString(ref b)   => str::from_utf8(b).ok(),
      Frame::SimpleString(ref s) => Some(s),
      Frame::Error(ref s)        => Some(s),
      _                          => None
    }
  }

}
impl PartialEq<&str> for Frame {
  fn eq(&self, other: &&str) -> bool {
    match self {
      Frame::SimpleString(s) => s.eq(other),
      Frame::BulkString(s) => s.eq(other),
      _ => false,
    }
  }
}

impl fmt::Display for Frame {
  fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
    match self {
      Frame::SimpleString(response) => response.fmt(fmt),
      Frame::Error(msg) => write!(fmt, "error: {}", msg),
      Frame::Integer(num) => num.fmt(fmt),
      Frame::BulkString(msg) => match str::from_utf8(msg) {
        Ok(string) => string.fmt(fmt),
        Err(_) => write!(fmt, "{:?}", msg),
      },
      Frame::Null => "(nil)".fmt(fmt),
      Frame::Array(parts) => {
        for (i, part) in parts.iter().enumerate() {
          if i > 0 {
            write!(fmt, " ")?;
            part.fmt(fmt)?;
          }
        }

        Ok(())
      }
      Frame::Moved(response) => response.fmt(fmt),
      Frame::Ask(response) => response.fmt(fmt)
    }
  }
}

fn peek_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
  if !src.has_remaining() {
    return Err(Error::Incomplete);
  }

  Ok(src.bytes()[0])
}

fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
  if !src.has_remaining() {
    return Err(Error::Incomplete);
  }

  Ok(src.get_u8())
}

fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), Error> {
  if src.remaining() < n {
    return Err(Error::Incomplete);
  }

  src.advance(n);
  Ok(())
}

/// Read a new-line terminated decimal
fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<i64, Error> {
  let line = get_line(src)?;

  atoi::<i64>(line).ok_or_else(|| "protocol error; invalid frame format".into())
}

/// Find a line
fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
  // Scan the bytes directly
  let start = src.position() as usize;
  // Scan to the second to last byte
  let end = src.get_ref().len() - 1;

  for i in start..end {
    if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
      // We found a line, update the position to be *after* the \n
      src.set_position((i + 2) as u64);

      // Return the line
      return Ok(&src.get_ref()[start..i]);
    }
  }

  Err(Error::Incomplete)
}

#[derive(Debug)]
pub enum Error {
  /// Not enough data is available to parse a message
  Incomplete,

  /// Invalid message encoding
  Other(anyhow::Error),
}


impl From<String> for Error {
  fn from(src: String) -> Error {
    Error::Other(anyhow!("{}", src))
  }
}

impl From<&str> for Error {
  fn from(src: &str) -> Error {
    src.to_string().into()
  }
}

impl From<FromUtf8Error> for Error {
  fn from(_src: FromUtf8Error) -> Error {
    "protocol error; invalid frame format".into()
  }
}

impl From<TryFromIntError> for Error {
  fn from(_src: TryFromIntError) -> Error {
    "protocol error; invalid frame format".into()
  }
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
  fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
    match self {
      Error::Incomplete => "stream ended early".fmt(fmt),
      Error::Other(err) => err.fmt(fmt),
    }
  }
}

impl From<Redirection> for Frame {
  fn from(redirection: Redirection) -> Self {
    match redirection {
      Redirection::Moved {ref slot, ref host, ref port} => Frame::Moved(utils::redirection_to_frame("MOVED", *slot, host, *port)),
      Redirection::Ask {ref slot, ref host, ref port}   => Frame::Ask(utils::redirection_to_frame("ASK", *slot, host, *port))
    }
  }
}
