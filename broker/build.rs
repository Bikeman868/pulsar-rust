use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use uuid::Uuid;

fn main() {
    let build_number = Uuid::new_v4();
    let build_number = format!("pub const BUILD_NUMBER: &str = \"{build_number}\";\n");

    let write = OpenOptions::new().write(true).create(true).open(r#"./src/build_number.rs"#).unwrap();
    let mut writer = BufWriter::new(write);

    let buffer = build_number.as_bytes();
    writer.write(buffer).unwrap();
}