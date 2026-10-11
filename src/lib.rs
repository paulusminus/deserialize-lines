#![doc = include_str!("../README.md")]

use crate::object_stream::ObjectsStream;
use futures_util::{
    AsyncBufRead,
    io::{AllowStdIo, BufReader},
};
use std::{
    error::Error,
    io::{Read, Result},
};

mod object_stream;

pub type Parser<O> = fn(String) -> Result<O>;

pub fn convert<O>(f: Parser<O>) -> impl Fn(Result<String>) -> Result<O> {
    move |r| r.and_then(f)
}

/// Make anything that implements [`std::io::Read`] a Async BufReader
pub trait IntoAsyncBufRead: Read + Sized {
    fn into_async_bufreader(self) -> BufReader<AllowStdIo<Self>>;
}

impl<R: Read + Sized> IntoAsyncBufRead for R {
    fn into_async_bufreader(self) -> BufReader<AllowStdIo<Self>> {
        BufReader::new(AllowStdIo::new(self))
    }
}

/// Convert std::error::Error error to std::io::Error error
pub trait ErrIntoIO<T> {
    fn err_into_io(self) -> Result<T>;
}

impl<E: Error + Send + Sync + 'static, T> ErrIntoIO<T> for std::result::Result<T, E> {
    fn err_into_io(self) -> Result<T> {
        self.map_err(std::io::Error::other)
    }
}

/// DeserializeLines is now a trait. It makes chaining easier.
pub trait DeserializeLines: AsyncBufRead + Sized {
    fn deserialize_lines<O, F>(self, deserializer: F) -> ObjectsStream<O, Self, F>
    where
        F: Fn(Result<String>) -> Result<O>;
}

impl<R: AsyncBufRead + Sized> DeserializeLines for R {
    fn deserialize_lines<O, F>(self, deserializer: F) -> ObjectsStream<O, Self, F>
    where
        F: Fn(Result<String>) -> Result<O>,
    {
        ObjectsStream::new(self, deserializer)
    }
}

#[cfg(test)]
mod test {
    use std::fs::File;

    use crate::{IntoAsyncBufRead, Parser, object_stream::ObjectsStream};

    use super::{DeserializeLines, ErrIntoIO, Result, convert};
    use futures_util::{StreamExt, TryStreamExt};
    use serde::Deserialize;
    use serde_json::from_str;

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct Person {
        name: String,
        age: Option<u32>,
    }

    #[test]
    fn test_convert() {
        let to_person: Parser<Person> = |s| from_str(&s).err_into_io();
        let converter = |r: Result<String>| r.and_then(to_person);
        let _ = ObjectsStream::new("".as_bytes(), converter);
    }

    #[tokio::test]
    async fn deserialize_single_line() {
        let to_person: Parser<Person> = |s| from_str(&s).err_into_io();
        let deserializer = convert(to_person);
        let persons = "{\"name\": \"Paul Min\"}"
            .as_bytes()
            .deserialize_lines(deserializer)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let paul = persons.get(0).unwrap();
        assert_eq!(paul.name, *"Paul Min");
        assert_eq!(paul.age, None);
    }

    #[tokio::test]
    async fn deserialize_multiple_lines() {
        let to_person: Parser<Person> = |s| from_str(&s).err_into_io();
        let persons =
            "{\"name\": \"Paul Min\", \"age\": 30}\n{\"name\": \"John Doe\", \"age\": 25}"
                .as_bytes()
                .deserialize_lines(convert(to_person))
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
    async fn deserialize_file() {
        let lines = File::open("test.ndjson").unwrap().into_async_bufreader();
        let to_person: Parser<Person> = |s| from_str(&s).err_into_io();
        let mut persons = lines.deserialize_lines(convert(to_person)).boxed();
        let first_person = persons.try_next().await.unwrap().unwrap();
        assert_eq!(first_person.name, *"Paul Min");
    }

    #[tokio::test]
    async fn deserialize_sync_converter() {
        let to_person: Parser<Person> = |s| from_str(&s).err_into_io();
        let persons = "{\"name\": \"Paul Min\"}"
            .as_bytes()
            .deserialize_lines(convert(to_person))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let paul = persons.get(0).unwrap();
        assert_eq!(paul.name, *"Paul Min");
        assert_eq!(paul.age, None);
    }
}
