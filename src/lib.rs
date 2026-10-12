#![doc = include_str!("../README.md")]

use crate::object_stream::ObjectsStream;
use futures_util::{
    AsyncBufRead,
    io::{AllowStdIo, BufReader},
};
use std::io::{Read, Result};

mod object_stream;

pub type Parser<O> = fn(String) -> Result<O>;

/// Make anything that implements [std::io::Read] a Async BufReader
pub trait IntoAsyncBufRead: Read + Sized {
    fn into_async_bufreader(self) -> BufReader<AllowStdIo<Self>> {
        BufReader::new(AllowStdIo::new(self))
    }
}

impl<R: Read + Sized> IntoAsyncBufRead for R {}

/// Convert std::error::Error error to std::io::Error error
pub trait ErrIntoIO<T> {
    fn err_into_io(self) -> Result<T>;
}

impl<E: Into<Box<dyn std::error::Error + Send + Sync>>, T> ErrIntoIO<T>
    for std::result::Result<T, E>
{
    fn err_into_io(self) -> Result<T> {
        self.map_err(std::io::Error::other)
    }
}

/// DeserializeLines is now a trait. It makes chaining easier.
pub trait ParseLines: AsyncBufRead + Sized {
    fn parse_lines<O>(self, deserializer: Parser<O>) -> ObjectsStream<O, Self> {
        ObjectsStream::new(self, deserializer)
    }
}

impl<R: AsyncBufRead + Sized> ParseLines for R {}

#[cfg(test)]
mod test {
    use super::{ErrIntoIO, ParseLines};
    use crate::{IntoAsyncBufRead, Parser, object_stream::ObjectsStream};
    use futures_util::{StreamExt, TryStreamExt};
    use serde::Deserialize;
    use serde_json::from_str;
    use std::fs::File;

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct Person {
        name: String,
        age: Option<u32>,
    }

    #[tokio::test]
    async fn parse_empty() {
        let mut s = ObjectsStream::new("".as_bytes(), |s| from_str::<Person>(&s).err_into_io());
        let n = s.try_next().await.unwrap();
        assert_eq!(n, None);
    }

    #[tokio::test]
    async fn parse_single_line() {
        let to_person: Parser<Person> = |s| from_str(&s).err_into_io();
        let persons = "{\"name\": \"Paul Min\"}"
            .as_bytes()
            .parse_lines(to_person)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let paul = persons.get(0).unwrap();
        assert_eq!(paul.name, *"Paul Min");
        assert_eq!(paul.age, None);
    }

    #[tokio::test]
    async fn parse_multiple_lines() {
        let to_person: Parser<Person> = |s| from_str(&s).err_into_io();
        let persons =
            "{\"name\": \"Paul Min\", \"age\": 30}\n{\"name\": \"John Doe\", \"age\": 25}"
                .as_bytes()
                .parse_lines(to_person)
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
        assert_eq!(
            persons,
            vec![
                Person {
                    name: "Paul Min".to_string(),
                    age: Some(30),
                },
                Person {
                    name: "John Doe".to_string(),
                    age: Some(25),
                }
            ]
        );
    }

    #[tokio::test]
    async fn parse_file() {
        let lines = File::open("test.ndjson").unwrap().into_async_bufreader();
        let to_person: Parser<Person> = |s| from_str(&s).err_into_io();
        let mut persons = lines.parse_lines(to_person).boxed();
        let first_person = persons.try_next().await.unwrap().unwrap();
        assert_eq!(first_person.name, *"Paul Min");
    }
}
