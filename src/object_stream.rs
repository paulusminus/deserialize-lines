use crate::Parser;

use super::Result;
use futures_util::{AsyncBufRead, AsyncBufReadExt, Stream, io::Lines};
use std::{
    pin::Pin,
    task::{Context, Poll, ready},
};

#[pin_project::pin_project]
pub struct ObjectsStream<O, R> {
    #[pin]
    lines: Lines<R>,
    f: Parser<O>,
}

impl<O, R> ObjectsStream<O, R>
where
    R: AsyncBufRead,
{
    pub fn new(reader: R, parser: Parser<O>) -> Self {
        Self {
            lines: reader.lines(),
            f: parser,
        }
    }
}

impl<O, R> Stream for ObjectsStream<O, R>
where
    R: AsyncBufRead,
{
    type Item = Result<O>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        let res = ready!(this.lines.as_mut().poll_next(cx));
        Poll::Ready(res.map(|x| x.and_then(this.f)))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.lines.size_hint()
    }
}
