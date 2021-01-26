use std::{error::Error, fmt::Debug};

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {
    println!("Hello, world.");

    let a = Stream {
        value: String::from("ghi"),
    };
    println!("a: {:?}", a);
    println!("a: {:?}", a.map(length));

    Ok(())
}

fn length(i: String) -> usize {
    i.len()
}

#[derive(Debug)]
struct Stream<V> {
    value: V,
}

trait Mapper<T, U> {
    fn map(self, m: fn(T) -> U) -> Stream<U>;
}

impl<T, U> Mapper<T, U> for Stream<T> {
    fn map(self, m: fn(T) -> U) -> Stream<U> {
        Stream {
            value: m(self.value),
        }
    }
}
trait Brancher<T> {
    fn branch(self, size: usize, b: fn(T) -> usize) -> Vec<Stream<T>>;
}

impl<T> Brancher<T> for Stream<T> {
    fn branch(self, size: usize, m: fn(T) -> usize) -> Vec<Stream<T>> {
        Stream {
            value: m(self.value),
        }
    }
}
