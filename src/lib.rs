use futures_util::{
    AsyncBufRead, AsyncBufReadExt, Stream, TryStreamExt, io::Lines, stream::AndThen,
};
use std::{
    error::Error,
    io::Result,
    pin::Pin,
    task::{Context, Poll},
};

pub trait ErrIntoIO<T> {
    fn err_into_io(self) -> Result<T>;
}

impl<E: Error + Send + Sync + 'static, T> ErrIntoIO<T> for std::result::Result<T, E> {
    fn err_into_io(self) -> Result<T> {
        self.map_err(std::io::Error::other)
    }
}

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
            objects: self.lines().and_then(deserializer),
        }
    }
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

#[cfg(test)]
mod test {
    use super::{DeserializeLines, ErrIntoIO, Result};
    use futures_util::TryStreamExt;
    use serde::Deserialize;

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct Person {
        name: String,
        age: Option<u32>,
    }

    #[tokio::test]
    async fn deserialize_single_lines() {
        async fn deserialize(s: String) -> Result<Person> {
            serde_json::from_str(&s).err_into_io()
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
            serde_json::from_str(&s).err_into_io()
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
}
