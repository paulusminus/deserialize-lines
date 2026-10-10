# json-lines

A rust crate for reading JSON lines from a file or a string.

## Example

```
  #[tokio::main]
  async fn main() {
      use serde::{Deserialize};
      use deserialize_lines::{DeserializeLines, ErrIntoIOError};
      use futures_util::{StreamExt, TryStreamExt};

      #[derive(Deserialize)]
      struct Person {
        name: String,
      }
      
      let s = "{\"name\": \"Paul Min\"}".as_bytes();
      let mut persons = s.deserialize_lines(|s| async move {
        serde_json::from_str::<Person>(&s).err_into_io_error()
      }).boxed();

      let person = persons.try_next().await.unwrap().unwrap();
      assert_eq!(person.name, *"Paul Min");
  }
```
