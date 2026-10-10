#![doc = include_str!("../README.md")]

use futures_util::{
    AsyncBufRead, AsyncBufReadExt, Stream, TryStreamExt, io::Lines, stream::AndThen,
};
use std::{
    error::Error,
    io::Result,
    pin::Pin,
    task::{Context, Poll},
};

/// Convert std::error::Error error to std::io::Error error
pub trait ErrIntoIOError<T> {
    fn err_into_io_error(self) -> Result<T>;
}

impl<E: Error + Send + Sync + 'static, T> ErrIntoIOError<T> for std::result::Result<T, E> {
    fn err_into_io_error(self) -> Result<T> {
        self.map_err(std::io::Error::other)
    }
}

/// DeserializeLines is now a trait. It makes chaining easier.
pub trait DeserializeLines: AsyncBufRead + Sized {
    fn deserialize_lines<O, F, Fut>(self, deserializer: F) -> DeserializedStream<O, Self, F, Fut>
    where
        F: Fn(String) -> Fut,
        Fut: Future<Output = Result<O>>;
}

impl<R: AsyncBufRead + Sized> DeserializeLines for R {
    fn deserialize_lines<O, F, Fut>(self, deserializer: F) -> DeserializedStream<O, R, F, Fut>
    where
        F: Fn(String) -> Fut,
        Fut: Future<Output = Result<O>>,
    {
        DeserializedStream {
            objects: self.lines().and_then(deserializer),
        }
    }
}

/// The result of asynchronously reading lines and deserializing them
#[pin_project::pin_project]
pub struct DeserializedStream<
    O,
    R: AsyncBufRead,
    F: Fn(String) -> Fut,
    Fut: Future<Output = Result<O>>,
> {
    #[pin]
    objects: AndThen<Lines<R>, Fut, F>,
}

impl<R, O, F, Fut> Stream for DeserializedStream<O, R, F, Fut>
where
    R: AsyncBufRead,
    F: Fn(String) -> Fut,
    Fut: Future<Output = Result<O>>,
{
    type Item = Result<O>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().objects.poll_next(cx)
    }
}

#[cfg(test)]
mod test {
    use std::fs::File;

    use super::{DeserializeLines, ErrIntoIOError, Result};
    use futures_util::{StreamExt, TryStreamExt, io::AllowStdIo};
    use serde::Deserialize;

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct Person {
        name: String,
        age: Option<u32>,
    }

    #[tokio::test]
    async fn deserialize_single_lines() {
        async fn deserialize(s: String) -> Result<Person> {
            serde_json::from_str(&s).err_into_io_error()
        }
        let persons = "{\"name\": \"Paul Min\"}"
            .as_bytes()
            .deserialize_lines(deserialize)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let paul = persons.get(0).unwrap();
        assert_eq!(paul.name, *"Paul Min");
        assert_eq!(paul.age, None);
    }

    #[tokio::test]
    async fn test_deserialize_lines() {
        async fn deserialize(s: String) -> Result<Person> {
            serde_json::from_str(&s).err_into_io_error()
        }
        let persons =
            "{\"name\": \"Paul Min\", \"age\": 30}\n{\"name\": \"John Doe\", \"age\": 25}"
                .as_bytes()
                .deserialize_lines(deserialize)
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
    async fn test_file() {
        let f = File::open("test.ndjson").unwrap();
        let lines = futures_util::io::BufReader::new(AllowStdIo::new(f));
        let mut persons = lines
            .deserialize_lines(
                |s| async move { serde_json::from_str::<Person>(&s).err_into_io_error() },
            )
            .boxed();
        let first_person = persons.try_next().await.unwrap().unwrap();
        assert_eq!(first_person.name, *"Paul Min");
    }
}
