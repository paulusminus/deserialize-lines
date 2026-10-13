# parse-lines

A rust crate for reading and parsing lines from anything implementing [`AsyncBufRead`](https://docs.rs/futures-io/latest/futures_io/trait.AsyncBufRead.html) from the futures crate.

## Example

```
  use futures_util::{TryStreamExt};
  use parse_lines::ParseLines;
  use serde::{Deserialize};
  use serde_json::from_str;
  use std::pin::pin;

  #[derive(Deserialize)]
  struct Person {
    name: String,
  }

  #[tokio::main]
  async fn main() {    
      let source = "{\"name\": \"Paul Min\"}".as_bytes();
      let persons = source.parse_lines(|s| from_str::<Person>(&s));

      let mut pinned = pin!(persons);
      let person = pinned.try_next().await.unwrap().unwrap();
      assert_eq!(person.name, *"Paul Min");
  }
```
