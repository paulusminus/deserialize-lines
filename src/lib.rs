#![doc = include_str!("../README.md")]

use futures_util::{
    AsyncBufRead, AsyncBufReadExt, Stream, StreamExt,
    io::{AllowStdIo, BufReader, Lines},
    stream::Map,
};
use std::{
    error::Error,
    io::{Read, Result},
    pin::Pin,
    task::{Context, Poll},
};

/// Make anything that implements [`std::io::Read`] a Async BufReader
pub trait IntoAsyncBufRead: Read + Sized {
    fn into_async_bufreader(self) -> BufReader<AllowStdIo<Self>>;
}

impl<R: Read + Sized> IntoAsyncBufRead for R {
    fn into_async_bufreader(self) -> BufReader<AllowStdIo<Self>> {
        let io = AllowStdIo::new(self);
        BufReader::new(io)
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
    fn deserialize_lines<O, F>(self, deserializer: F) -> ObjectsStream<Self, F>
    where
        F: Fn(Result<String>) -> Result<O>;
}

impl<R: AsyncBufRead + Sized> DeserializeLines for R {
    fn deserialize_lines<O, F>(self, deserializer: F) -> ObjectsStream<Self, F>
    where
        F: Fn(Result<String>) -> Result<O>,
    {
        ObjectsStream {
            inner: self.lines().map(deserializer),
        }
    }
}

#[pin_project::pin_project]
pub struct ObjectsStream<R, F> {
    #[pin]
    inner: Map<Lines<R>, F>,
}

impl<O, R, F> Stream for ObjectsStream<R, F>
where
    F: Fn(Result<String>) -> Result<O>,
    R: AsyncBufRead,
{
    type Item = Result<O>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }
}
#[cfg(test)]
mod test {
    use std::fs::File;

    use super::{DeserializeLines, ErrIntoIO, IntoAsyncBufRead};
    use futures_util::{StreamExt, TryStreamExt};
    use serde::Deserialize;

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct Person {
        name: String,
        age: Option<u32>,
    }

    #[tokio::test]
    async fn deserialize_single_line() {
        let persons = "{\"name\": \"Paul Min\"}"
            .as_bytes()
            .deserialize_lines(|r| r.and_then(|s| serde_json::from_str::<Person>(&s).err_into_io()))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let paul = persons.get(0).unwrap();
        assert_eq!(paul.name, *"Paul Min");
        assert_eq!(paul.age, None);
    }

    #[tokio::test]
    async fn deserialize_multiple_lines() {
        let persons =
            "{\"name\": \"Paul Min\", \"age\": 30}\n{\"name\": \"John Doe\", \"age\": 25}"
                .as_bytes()
                .deserialize_lines(|r| {
                    r.and_then(|s| serde_json::from_str::<Person>(&s).err_into_io())
                })
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
        let mut persons = lines
            .deserialize_lines(|r| r.and_then(|s| serde_json::from_str::<Person>(&s).err_into_io()))
            .boxed();
        let first_person = persons.try_next().await.unwrap().unwrap();
        assert_eq!(first_person.name, *"Paul Min");
    }

    #[tokio::test]
    async fn deserialize_sync_converter() {
        let persons = "{\"name\": \"Paul Min\"}"
            .as_bytes()
            .deserialize_lines(|r| r.and_then(|s| serde_json::from_str::<Person>(&s).err_into_io()))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let paul = persons.get(0).unwrap();
        assert_eq!(paul.name, *"Paul Min");
        assert_eq!(paul.age, None);
    }
}
