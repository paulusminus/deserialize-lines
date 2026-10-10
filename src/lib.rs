use futures_util::{Stream, TryStreamExt, stream::AndThen};
use std::{
    io::Result,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncBufRead, AsyncBufReadExt};
use tokio_stream::wrappers::LinesStream;

/// DeserializeLines is now a trait. It makes chaining easier.
pub trait DeserializeLines {
    fn deserialize_lines<O, F, Fut>(self, deserializer: F) -> DeserializedStream<O, Self, F, Fut>
    where
        Self: AsyncBufRead + Sized,
        F: Fn(String) -> Fut,
        Fut: Future<Output = Result<O>>;
}

impl<R: AsyncBufRead> DeserializeLines for R {
    fn deserialize_lines<O, F, Fut>(self, deserializer: F) -> DeserializedStream<O, R, F, Fut>
    where
        Self: AsyncBufRead + Sized,
        F: Fn(String) -> Fut,
        Fut: Future<Output = Result<O>>,
    {
        DeserializedStream {
            objects: Lines::from(self).and_then(deserializer),
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

impl<R: AsyncBufRead> Stream for Lines<R> {
    type Item = Result<String>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().reader.poll_next(cx)
    }
}

impl<R> From<R> for Lines<R>
where
    R: AsyncBufRead,
{
    fn from(reader: R) -> Self {
        Self {
            reader: LinesStream::new(reader.lines()),
        }
    }
}

#[cfg(test)]
mod test {
    use futures_util::future::ready;
    use std::io::Error;

    use super::DeserializeLines;
    use futures_util::TryStreamExt;

    #[tokio::test]
    async fn test_deserialize_lines_short() {
        use serde_json::Value;

        let mut values = "{\"name\": \"Paul Min\"}"
            .as_bytes()
            .deserialize_lines(|s| ready(serde_json::from_str::<Value>(&s).map_err(Error::other)))
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
                .deserialize_lines(|s| {
                    ready(serde_json::from_str::<Person>(&s).map_err(Error::other))
                })
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
