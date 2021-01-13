
use ::types::*;

use bytes::BytesMut;

/// Attempt to parse the contents of `buf`, returning the first valid frame and the number of bytes consumed.
/// If the byte slice contains an incomplete frame then `None` is returned.
pub fn decode(buf: &[u8]) -> Result<(Option<Frame>, usize), Error> {
  let mut cursor = std::io::Cursor::new(buf);
  let start = cursor.position();

  match Frame::parse(&mut cursor) {
    Ok(f) => {
      Ok((Some(f), (cursor.position() - start) as usize))
    }
    Err(Error::Incomplete) => {
      Ok((None, 0))
    }
    Err(Error::Other(e)) => {
      Err(Error::Other(e))
    }
  }
}

/// Attempt to parse the contents of `buf`, returning the first valid frame and the number of bytes consumed.
/// If the byte slice contains an incomplete frame then `None` is returned.
///
/// **The caller is responsible for consuming the underlying bytes.**
pub fn decode_bytes(buf: & BytesMut) -> Result<(Option<Frame>, usize), Error> {
  decode(buf)
}


#[cfg(test)]
mod tests {
  use super::*;
  use ::utils;
  use ::types::*;

  use std::fmt;
  use std::str;

  use bytes::Bytes;

  const PADDING: &'static str = "FOOBARBAZ";

  fn str_to_bytes(s: &str) -> Bytes {
    Bytes::from(s.as_bytes().to_vec())

  }

  fn to_bytes(s: &str) -> BytesMut {
    BytesMut::from(s)
  }

  fn empty_bytes() -> BytesMut {
    BytesMut::new()
  }

  fn pretty_print_panic(e: Error) {
    panic!("{:?}", e);
  }

  fn decode_and_verify_some(bytes: &mut BytesMut, expected: &(Option<Frame>, usize)) {
    let (frame, len) = match decode_bytes(bytes) {
      Ok((f, l)) => (f, l),
      Err(e) => return pretty_print_panic(e)
    };

    assert_eq!(frame, expected.0, "decoded frame matched");
    assert_eq!(len, expected.1, "decoded frame len matched");
  }

  fn decode_and_verify_padded_some(bytes: &mut BytesMut, expected: &(Option<Frame>, usize)) {
    bytes.extend_from_slice(PADDING.as_bytes());

    let (frame, len) = match decode_bytes(bytes) {
      Ok((f, l)) => (f, l),
      Err(e) => return pretty_print_panic(e)
    };

    assert_eq!(frame, expected.0, "decoded frame matched");
    assert_eq!(len, expected.1, "decoded frame len matched");
  }

  fn decode_and_verify_none(bytes: &mut BytesMut) {
    let (frame, len) = match decode_bytes(bytes) {
      Ok((f, l)) => (f, l),
      Err(e) => return pretty_print_panic(e)
    };

    assert!(frame.is_none());
    assert_eq!(len, 0);
  }

  #[test]
  fn should_decode_llen_res_example() {
    let expected = (Some(Frame::Integer(48293)), 8);
    let mut bytes: BytesMut = ":48293\r\n".into();

    decode_and_verify_some(&mut bytes, &expected);
    decode_and_verify_padded_some(&mut bytes, &expected);
  }

  #[test]
  fn should_decode_simple_string() {
    let expected = (Some(Frame::SimpleString("string".into())), 9);
    let mut bytes: BytesMut = "+string\r\n".into();

    decode_and_verify_some(&mut bytes, &expected);
    decode_and_verify_padded_some(&mut bytes, &expected);
  }

  #[test]
  fn should_decode_bulk_string() {
    let expected = (Some(Frame::BulkString(str_to_bytes("foo"))), 9);
    let mut bytes: BytesMut = "$3\r\nfoo\r\n".into();

    decode_and_verify_some(&mut bytes, &expected);
    decode_and_verify_padded_some(&mut bytes, &expected);
  }

  #[test]
  fn should_decode_array_no_nulls() {
    let expected = (Some(Frame::Array(vec![
      Frame::SimpleString("Foo".into()),
      Frame::SimpleString("Bar".into())
    ])), 16);
    let mut bytes: BytesMut = "*2\r\n+Foo\r\n+Bar\r\n".into();

    decode_and_verify_some(&mut bytes, &expected);
    decode_and_verify_padded_some(&mut bytes, &expected);
  }

  #[test]
  fn should_decode_array_nulls() {
    let mut bytes: BytesMut = "*3\r\n$3\r\nFoo\r\n$-1\r\n$3\r\nBar\r\n".into();

    let expected = (Some(Frame::Array(vec![
      Frame::BulkString(str_to_bytes("Foo")),
      Frame::Null,
      Frame::BulkString(str_to_bytes("Bar"))
    ])), bytes.len());

    decode_and_verify_some(&mut bytes, &expected);
    decode_and_verify_padded_some(&mut bytes, &expected);
  }

  #[test]
  fn should_decode_normal_error() {
    let mut bytes: BytesMut = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".into();
    let expected = (Some(Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into())), bytes.len());

    decode_and_verify_some(&mut bytes, &expected);
    decode_and_verify_padded_some(&mut bytes, &expected);
  }

  #[test]
  fn should_decode_moved_error() {
    let mut bytes: BytesMut = "-MOVED 3999 127.0.0.1:6381\r\n".into();
    let expected = (Some(Frame::Moved{slot: 3999, host: "127.0.0.1".to_string(), port: 6381}), bytes.len());

    decode_and_verify_some(&mut bytes, &expected);
    decode_and_verify_padded_some(&mut bytes, &expected);
  }

  #[test]
  fn should_decode_ask_error() {
    let mut bytes: BytesMut = "-ASK 3999 127.0.0.1:6381\r\n".into();
    let expected = (Some(Frame::Ask{slot: 3999, host: "127.0.0.1".to_string(), port: 6381}), bytes.len());

    decode_and_verify_some(&mut bytes, &expected);
    decode_and_verify_padded_some(&mut bytes, &expected);
  }

  #[test]
  fn should_decode_incomplete() {
    let mut bytes: BytesMut = "*3\r\n$3\r\nFoo\r\n$-1\r\n$3\r\nBar".into();
    decode_and_verify_none(&mut bytes);
  }

  #[test]
  #[should_panic]
  fn should_error_on_junk() {
    let mut bytes: BytesMut = "foobarbazwibblewobble".into();
    let _ = decode_bytes(&bytes).map_err(|e| pretty_print_panic(e));
  }

}
