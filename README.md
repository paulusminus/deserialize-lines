# json-lines

A rust crate for reading and parsing lines from anything implementing [`AsyncBufRead`](futures_util::AsyncBufRead).

## Example

```
  use parse_lines::{ParseLines, ErrIntoIO, Parser};
  use futures_util::{StreamExt, TryStreamExt};
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
      let to_person: Parser<Person> = |s| from_str(&s).err_into_io(); 
      let persons = source.parse_lines(to_person);

      let mut pinned = pin!(persons);
      let person = pinned.try_next().await.unwrap().unwrap();
      assert_eq!(person.name, *"Paul Min");
  }
```
