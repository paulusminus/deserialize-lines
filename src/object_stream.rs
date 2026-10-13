use super::{BoxedError, IOResult};
use futures_util::{AsyncBufRead, AsyncBufReadExt, Stream, io::Lines};
use pin_project::pin_project;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

#[pin_project]
pub struct ObjectsStream<O, R, E, F>
where
    F: FnMut(String) -> Result<O, E>,
    E: Into<BoxedError>,
{
    #[pin]
    lines: Lines<R>,
    parser: F,
}

impl<O, R, E, F> ObjectsStream<O, R, E, F>
where
    R: AsyncBufRead,
    F: FnMut(String) -> Result<O, E>,
    E: Into<BoxedError>,
{
    pub fn new(reader: R, parser: F) -> Self {
        Self {
            lines: reader.lines(),
            parser,
        }
    }
}

impl<O, R, E, F> Stream for ObjectsStream<O, R, E, F>
where
    R: AsyncBufRead,
    F: FnMut(String) -> Result<O, E>,
    E: Into<BoxedError>,
{
    type Item = IOResult<O>;
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
