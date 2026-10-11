# json-lines

A rust crate for reading JSON lines from anything implementing [futures_util::AsyncBufRead].

## Example

```
  use deserialize_lines::{DeserializeLines, ErrIntoIO};
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
      let s = "{\"name\": \"Paul Min\"}".as_bytes();
      let persons = s.deserialize_lines(|r| r.and_then(|s| from_str::<Person>(&s).err_into_io()));

      let mut pinned = pin!(persons);
      let person = pinned.try_next().await.unwrap().unwrap();
      assert_eq!(person.name, *"Paul Min");
  }
```
