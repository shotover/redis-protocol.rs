use ::utils;
use anyhow::Result;
use bytes::BufMut;
use bytes::BytesMut;
use ::types::*;

use std::io::Cursor;
use std::io;

/// Write a single `Frame` value to the underlying stream.
    ///
    /// The `Frame` value is written to the socket using the various `write_*`
    /// functions provided by `AsyncWrite`. Calling these functions directly on
    /// a `TcpStream` is **not** advised, as this will result in a large number of
    /// syscalls. However, it is fine to call these functions on a *buffered*
    /// write stream. The data will be written to the buffer. Once the buffer is
    /// full, it is flushed to the underlying socket.
pub fn write_frame(stream: & mut BytesMut, frame: &Frame) -> io::Result<()> {
  // Arrays are encoded by encoding each entry. All other frame types are
  // considered literals. For now, mini-redis is not able to encode
  // recursive frame structures. See below for more details.
  // let start = stream.
  match frame {
    Frame::Array(val) => {
      // Encode the frame type prefix. For an array, it is `*`.
      stream.put_u8(b'*');

      // Encode the length of the array.
      write_decimal(stream, val.len() as i64)?;

      // Iterate and encode each entry in the array.
      for entry in &**val {
        write_value(stream, entry)?;
      }
    }
    // The frame type is a literal. Encode the value directly.
    _ => write_value(stream, frame)?,
  }

  // Ensure the encoded frame is written to the socket. The calls above
  // are to the buffered stream and writes. Calling `flush` writes the
  // remaining contents of the buffer to the socket.
  Ok(())
}

/// Write a frame literal to the stream
fn write_value(stream: & mut BytesMut, frame: &Frame) -> io::Result<()> {
  match frame {
    Frame::SimpleString(val) => {
      stream.put_u8(b'+');
      stream.put_slice(val.as_bytes());
      stream.put_slice(b"\r\n");
    }
    Frame::Error(val) => {
      stream.put_u8(b'-');
      stream.put_slice(val.as_bytes());
      stream.put_slice(b"\r\n");
    }
    Frame::Integer(val) => {
      stream.put_u8(b':');
      write_decimal(stream, *val)?;
    }
    Frame::Null => {
      stream.put_slice(b"$-1\r\n");
    }
    Frame::BulkString(val) => {
      let len = val.len();

      stream.put_u8(b'$');
      write_decimal(stream, len as i64)?;
      stream.put_slice(val);
      stream.put_slice(b"\r\n");
    }
    // Encoding an `Array` from within a value cannot be done using a
    // recursive strategy. In general, async fns do not support
    // recursion. Mini-redis has not needed to encode nested arrays yet,
    // so for now it is skipped.
    Frame::Array(val) => {
      let len = val.len();
      stream.put_u8(b'*');
      write_decimal(stream, len as i64)?;
      // stream.put_slice(b"\r\n");

      for v in val {
        write_value(stream, v)?;
      }
    },
    Frame::Moved { slot, host, port } => {
      stream.put_u8(b'-');
      stream.put_slice(format!("MOVED {} {}:{}", slot, host, port).as_bytes());
      stream.put_slice(b"\r\n");
    }
    Frame::Ask { slot, host, port } => {
      stream.put_u8(b'-');
      stream.put_slice(format!("MOVED {} {}:{}", slot, host, port).as_bytes());
      stream.put_slice(b"\r\n");
    }
  }

  Ok(())
}

/// Write a decimal frame to the stream
fn write_decimal(stream: & mut BytesMut, val: i64) -> io::Result<()> {
  use std::io::Write;

  // Convert the value to a string
  let mut buf = [0u8; 12];
  let mut buf = Cursor::new(&mut buf[..]);
  write!(&mut buf, "{}", val)?;

  let pos = buf.position() as usize;
  stream.put_slice(&buf.get_ref()[..pos]);
  stream.put_slice(b"\r\n");

  Ok(())
}

/// Attempt to encode a frame into `buf`, assuming a starting offset of 0.
///
/// The caller is responsible for extending the buffer if a `RedisProtocolErrorKind::BufferTooSmall` is returned.
pub fn encode(buf: &mut BytesMut, frame: &Frame) -> Result<usize> {
  write_frame(buf, frame)?;
  Ok(buf.len())
}

#[cfg(test)]
mod tests {
  use super::*;
  use ::utils::*;
  use ::types::*;
  use bytes::Bytes;

  const PADDING: &'static str = "foobar";

  fn str_to_bytes(s: &str) -> Bytes {
    Bytes::from(s.as_bytes().to_vec())

  }

  fn to_bytes(s: &str) -> BytesMut {
    BytesMut::from(s)
  }

  fn empty_bytes() -> BytesMut {
    BytesMut::new()
  }

  fn encode_and_verify_empty(input: &Frame, expected: &str) {
    let mut buf = empty_bytes();

    let len = match encode(&mut buf, input) {
      Ok(l) => l,
      Err(e) => panic!("{:?}", e)
    };

    assert_eq!(buf, expected.as_bytes(), "empty buf contents match");
    assert_eq!(len, expected.as_bytes().len(), "empty expected len is correct");
  }

  fn encode_and_verify_non_empty(input: &Frame, expected: &str) {
    let mut buf = empty_bytes();
    buf.extend_from_slice(PADDING.as_bytes());

    let len = match encode(&mut buf, input) {
      Ok(l) => l,
      Err(e) => panic!("{:?}", e)
    };
    let padded = vec![PADDING, expected].join("");

    assert_eq!(buf, padded.as_bytes(), "padded buf contents match");
    assert_eq!(len, padded.as_bytes().len(), "padded expected len is correct");
  }

  fn encode_raw_and_verify_empty(input: &Frame, expected: &str) {
    let mut buf = BytesMut::new();

    let len = match encode(&mut buf, input) {
      Ok(l) => l,
      Err(e) => panic!("{:?}", e)
    };

    assert_eq!(buf, expected.as_bytes(), "empty buf contents match");
    assert_eq!(len, expected.as_bytes().len(), "empty expected len is correct");
  }

  #[test]
  fn should_encode_llen_req_example() {
    let expected = "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n";
    let input = Frame::Array(vec![
      Frame::BulkString(str_to_bytes("LLEN")),
      Frame::BulkString(str_to_bytes("mylist"))
    ]);

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_incr_req_example() {
    let expected = "*2\r\n$4\r\nINCR\r\n$5\r\nmykey\r\n";
    let input = Frame::Array(vec![
      Frame::BulkString(str_to_bytes("INCR")),
      Frame::BulkString(str_to_bytes("mykey"))
    ]);

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_bitcount_req_example() {
    let expected = "*2\r\n$8\r\nBITCOUNT\r\n$5\r\nmykey\r\n";
    let input = Frame::Array(vec![
      Frame::BulkString(str_to_bytes("BITCOUNT")),
      Frame::BulkString(str_to_bytes("mykey"))
    ]);

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_array_bulk_string_test() {
    let expected = "*3\r\n$5\r\nWATCH\r\n$6\r\nWIBBLE\r\n$9\r\nfooBARbaz\r\n";
    let input = Frame::Array(vec![
      Frame::BulkString(str_to_bytes("WATCH")),
      Frame::BulkString(str_to_bytes("WIBBLE")),
      Frame::BulkString(str_to_bytes("fooBARbaz"))
    ]);

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_array_nested_test() {
    let expected = "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n";
    let input = Frame::Array(vec![
      Frame::Array(vec![
        Frame::Integer(1),
        Frame::Integer(2),
        Frame::Integer(3)
      ]),
      Frame::Array(vec![
        Frame::SimpleString("Foo".to_string()),
        Frame::Error("Bar".to_string()),
      ]),
    ]);

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_array_null_test() {
    let expected = "*3\r\n$4\r\nHSET\r\n$3\r\nfoo\r\n$-1\r\n";
    let input = Frame::Array(vec![
      Frame::BulkString(str_to_bytes("HSET")),
      Frame::BulkString(str_to_bytes("foo")),
      Frame::Null
    ]);

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_raw_llen_req_example() {
    let expected = "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n";
    let input = Frame::Array(vec![
      Frame::BulkString(str_to_bytes("LLEN")),
      Frame::BulkString(str_to_bytes("mylist"))
    ]);

    encode_raw_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_raw_incr_req_example() {
    let expected = "*2\r\n$4\r\nINCR\r\n$5\r\nmykey\r\n";
    let input = Frame::Array(vec![
      Frame::BulkString(str_to_bytes("INCR")),
      Frame::BulkString(str_to_bytes("mykey"))
    ]);

    encode_raw_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_raw_bitcount_req_example() {
    let expected = "*2\r\n$8\r\nBITCOUNT\r\n$5\r\nmykey\r\n";
    let input = Frame::Array(vec![
      Frame::BulkString(str_to_bytes("BITCOUNT")),
      Frame::BulkString(str_to_bytes("mykey"))
    ]);

    encode_raw_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_raw_array_bulk_string_test() {
    let expected = "*3\r\n$5\r\nWATCH\r\n$6\r\nWIBBLE\r\n$9\r\nfooBARbaz\r\n";
    let input = Frame::Array(vec![
      Frame::BulkString(str_to_bytes("WATCH")),
      Frame::BulkString(str_to_bytes("WIBBLE")),
      Frame::BulkString(str_to_bytes("fooBARbaz"))
    ]);

    encode_raw_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_raw_array_null_test() {
    let expected = "*3\r\n$4\r\nHSET\r\n$3\r\nfoo\r\n$-1\r\n";
    let input = Frame::Array(vec![
      Frame::BulkString(str_to_bytes("HSET")),
      Frame::BulkString(str_to_bytes("foo")),
      Frame::Null
    ]);

    encode_raw_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_moved_error() {
    let expected = "-MOVED 3999 127.0.0.1:6381\r\n";
    let input = Frame::Moved("MOVED 3999 127.0.0.1:6381".into());

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_ask_error() {
    let expected = "-ASK 3999 127.0.0.1:6381\r\n";
    let input = Frame::Ask("ASK 3999 127.0.0.1:6381".into());

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_error() {
    let expected = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    let input = Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into());

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_simplestring() {
    let expected = "+OK\r\n";
    let input = Frame::SimpleString("OK".into());

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_integer() {
    let i1_expected = ":1000\r\n";
    let i1_input = Frame::Integer(1000);

    encode_and_verify_empty(&i1_input, i1_expected);
    encode_and_verify_non_empty(&i1_input, i1_expected);
  }

  #[test]
  fn should_encode_negative_integer() {
    let i2_expected = ":-1000\r\n";
    let i2_input = Frame::Integer(-1000);

    encode_and_verify_empty(&i2_input, i2_expected);
    encode_and_verify_non_empty(&i2_input, i2_expected);
  }

}