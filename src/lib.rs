use futures_util::{
    Stream, TryStreamExt,
    future::{Ready, ready},
    stream::AndThen,
};
use serde::de::DeserializeOwned;
use std::{
    io::{Error, Result},
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncBufRead, AsyncBufReadExt};
use tokio_stream::wrappers::LinesStream;

type Deserializer<O> = fn(String) -> Ready<Result<O>>;

/// DeserializeLines is now a trait. It makes chaining easier.
pub trait DeserializeLines {
    fn deserialize_lines<O: DeserializeOwned>(self) -> Objects<O, Self>
    where
        Self: AsyncBufRead + Sized;
}

impl<R: AsyncBufRead> DeserializeLines for R {
    fn deserialize_lines<O: DeserializeOwned>(self) -> Objects<O, R>
    where
        Self: AsyncBufRead + Sized,
    {
        let converter: Deserializer<O> =
            |s| ready(serde_json::from_str::<O>(&s).map_err(Error::other));
        Objects {
            objects: Lines::from(self).and_then(converter),
        }
    }
}

#[pin_project::pin_project]
pub struct Lines<R: AsyncBufRead> {
    #[pin]
    reader: LinesStream<R>,
}

/// The result of asynchronously reading lines and converting them to objects
#[pin_project::pin_project]
pub struct Objects<O: DeserializeOwned, R: AsyncBufRead> {
    #[pin]
    objects: AndThen<Lines<R>, Ready<Result<O>>, Deserializer<O>>,
}

impl<R: AsyncBufRead, O: DeserializeOwned> Stream for Objects<O, R> {
    type Item = Result<O>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().objects.poll_next(cx)
    }
}

impl<R: AsyncBufRead> Stream for Lines<R> {
    type Item = Result<String>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().reader.poll_next(cx)
    }
}

impl<R: AsyncBufRead> From<R> for Lines<R> {
    fn from(reader: R) -> Self {
        Self {
            reader: LinesStream::new(reader.lines()),
        }
    }
}

#[cfg(test)]
mod test {
    use super::DeserializeLines;
    use futures_util::TryStreamExt;

    #[tokio::test]
    async fn test_deserialize_lines_short() {
        use serde_json::Value;

        let mut values = "{\"name\": \"Paul Min\"}"
            .as_bytes()
            .deserialize_lines::<Value>()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            values[0].take().as_object().unwrap().get("name").unwrap(),
            &Value::String("Paul Min".into())
        );
    }

    #[tokio::test]
    async fn test_deserialize_lines() {
        use serde::{Deserialize, Serialize};

        #[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
        struct Person {
            name: String,
            age: u32,
        }

        let persons =
            "{\"name\": \"Paul Min\", \"age\": 30}\n{\"name\": \"John Doe\", \"age\": 25}"
                .as_bytes()
                .deserialize_lines::<Person>()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
        assert_eq!(
            persons,
            vec![
                Person {
                    name: "Paul Min".to_string(),
                    age: 30
                },
                Person {
                    name: "John Doe".to_string(),
                    age: 25
                }
            ]
        );
    }
}
