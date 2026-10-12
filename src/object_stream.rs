use super::{Parser, Result};
use futures_util::{AsyncBufRead, AsyncBufReadExt, Stream, io::Lines};
use pin_project::pin_project;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

#[pin_project]
pub struct ObjectsStream<O, R, E> {
    #[pin]
    lines: Lines<R>,
    parser: Parser<O, E>,
}

impl<O, R, E> ObjectsStream<O, R, E>
where
    R: AsyncBufRead,
{
    pub fn new(reader: R, parser: Parser<O, E>) -> Self {
        Self {
            lines: reader.lines(),
            parser,
        }
    }
}

impl<O, R, E> Stream for ObjectsStream<O, R, E>
where
    R: AsyncBufRead,
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    type Item = Result<O>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.as_mut().project().lines.poll_next(cx).map(|opt| {
            opt.map(|r| {
                r.and_then(|s| {
                    (self.as_mut().project().parser)(s).map_err(|e| std::io::Error::other(e.into()))
                })
            })
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.lines.size_hint()
    }
}
