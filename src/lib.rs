#![doc = include_str!("../README.md")]

use crate::object_stream::ObjectsStream;
use futures_util::{
    AsyncBufRead,
    io::{AllowStdIo, BufReader},
};
use std::io::{Read, Result};

mod object_stream;

type Parser<O, E> = fn(String) -> std::result::Result<O, E>;

/// Make [`Read`] implement [`AsyncBufRead`] too
pub trait IntoAsyncBufRead: Read + Sized {
    fn into_async_bufreader(self) -> BufReader<AllowStdIo<Self>> {
        BufReader::new(AllowStdIo::new(self))
    }
}

impl<R: Read + Sized> IntoAsyncBufRead for R {}

/// Extension methods for [`AsyncBufRead`] providing [`parse_lines`](ParseLines::parse_lines)
pub trait ParseLines: AsyncBufRead + Sized {
    fn parse_lines<O, E>(self, parser: Parser<O, E>) -> ObjectsStream<O, Self, E> {
        ObjectsStream::new(self, parser)
    }
}

impl<R: AsyncBufRead + Sized> ParseLines for R {}

#[cfg(test)]
mod test {
    use super::ParseLines;
    use crate::{IntoAsyncBufRead, object_stream::ObjectsStream};
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
        let mut s = ObjectsStream::new("".as_bytes(), |s| from_str::<Person>(&s));
        let n = s.try_next().await.unwrap();
        assert_eq!(n, None);
    }

    #[tokio::test]
    async fn parse_single_line() {
        let persons = "{\"name\": \"Paul Min\"}"
            .as_bytes()
            .parse_lines(|s| from_str::<Person>(&s))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let paul = persons.get(0).unwrap();
        assert_eq!(paul.name, *"Paul Min");
        assert_eq!(paul.age, None);
    }

    #[tokio::test]
    async fn parse_multiple_lines() {
        let persons =
            "{\"name\": \"Paul Min\", \"age\": 30}\n{\"name\": \"John Doe\", \"age\": 25}"
                .as_bytes()
                .parse_lines(|s| from_str::<Person>(&s))
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
        let mut persons = lines.parse_lines(|s| from_str::<Person>(&s)).boxed();
        let first_person = persons.try_next().await.unwrap().unwrap();
        assert_eq!(first_person.name, *"Paul Min");
    }
}
