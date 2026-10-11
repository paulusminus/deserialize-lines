use super::Result;
use futures_util::{AsyncBufRead, AsyncBufReadExt, Stream, StreamExt, io::Lines, stream::Map};
use std::{
    pin::Pin,
    task::{Context, Poll},
};

#[pin_project::pin_project]
pub struct ObjectsStream<O, R, F>
where
    F: FnMut(Result<String>) -> Result<O>,
{
    #[pin]
    inner: Map<Lines<R>, F>,
}

impl<O, R, F> ObjectsStream<O, R, F>
where
    R: AsyncBufRead,
    F: Fn(Result<String>) -> Result<O>,
{
    pub fn new(reader: R, converter: F) -> Self {
        Self {
            inner: reader.lines().map(converter),
        }
    }
}

impl<O, R, F> Stream for ObjectsStream<O, R, F>
where
    R: AsyncBufRead,
    F: Fn(Result<String>) -> Result<O>,
{
    type Item = Result<O>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }
}
