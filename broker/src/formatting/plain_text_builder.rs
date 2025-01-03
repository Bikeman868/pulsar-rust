use pulsar_rust_net::data_types::Timestamp;

/// Implement this trait on structs that can be output in plain text
pub trait ToPlainText {
    /// Writes out the fields as plain text, usually in columns and all on one line with a line break at the end
    fn to_plain_text(self: &Self, builder: &mut PlainTextBuilder);

    /// Writes a line containing column headings with a line break at the end
    fn to_plain_text_header(builder: &mut PlainTextBuilder);
}

pub struct PlainTextBuilder {
    buffer: String,
    indent_level: usize,
    max_indent: usize,
    begining_of_line: bool,
}

impl PlainTextBuilder {
    pub fn new() -> Self {
        Self {
            buffer: String::with_capacity(5000),
            indent_level: 0,
            begining_of_line: true,
            max_indent: 2,
        }
    }

    pub fn build(self: Self) -> String {
        self.buffer
    }

    pub fn indent(self: &mut Self) {
        self.indent_level += 1
    }
    pub fn outdent(self: &mut Self) {
        self.indent_level -= 1
    }

    pub fn new_line(self: &mut Self) {
        if !self.begining_of_line {
            self.buffer.push_str("\n");
            self.begining_of_line = true;
        }
    }

    pub fn str_left(self: &mut Self, text: &str, width: usize) {
        if self.indent_level > self.max_indent {
            return;
        }

        self.check_for_indent();

        self.buffer.push_str(text);

        if text.len() < width {
            self.spaces(width - text.len());
        }
    }

    pub fn _str_right(self: &mut Self, text: &str, width: usize) {
        if self.indent_level > self.max_indent {
            return;
        }

        self.check_for_indent();

        if text.len() < width {
            self.spaces(width - text.len());
        }

        self.buffer.push_str(text);
    }

    pub fn usize_left(self: &mut Self, value: usize, width: usize) {
        self.str_left(&value.to_string(), width);
    }

    pub fn u16_left(self: &mut Self, value: u16, width: usize) {
        self.str_left(&value.to_string(), width);
    }

    pub fn u32_left(self: &mut Self, value: u32, width: usize) {
        self.str_left(&value.to_string(), width);
    }

    pub fn _u64_left(self: &mut Self, value: u64, width: usize) {
        self.str_left(&value.to_string(), width);
    }

    pub fn timestamp_left(self: &mut Self, value: Timestamp, width: usize) {
        self.str_left(&value.to_string(), width);
    }

    fn spaces(self: &mut Self, count: usize) {
        if count > 0 {
            for _ in 0..count {
                self.buffer.push_str(" ")
            }
        }
    }

    fn check_for_indent(self: &mut Self) {
        if self.begining_of_line {
            if self.indent_level > 0 {
                self.spaces(self.indent_level * 2);
            }
            self.begining_of_line = false;
        }
    }
}

mod tests {
    use super::*;

    struct TestData {
        a: String,
        b: usize,
        c: u32,
    }

    impl ToPlainText for TestData {
        fn to_plain_text_header(builder: &mut PlainTextBuilder) {
            builder.str_left("a", 10);
            builder.str_left("b", 10);
            builder.str_left("c", 10);
            builder.new_line();
        }

        fn to_plain_text(self: &Self, builder: &mut PlainTextBuilder) {
            builder.str_left(&self.a, 10);
            builder.usize_left(self.b, 10);
            builder.u32_left(self.c, 10);
            builder.new_line();
        }
    }

    #[test]
    fn format_columns() {
        let mut builder = PlainTextBuilder::new();

        TestData::to_plain_text_header(&mut builder);
        TestData {
            a: String::from("One"),
            b: 1,
            c: 1,
        }
        .to_plain_text(&mut builder);
        TestData {
            a: String::from("Two"),
            b: 2,
            c: 2,
        }
        .to_plain_text(&mut builder);
        TestData {
            a: String::from("Three"),
            b: 3,
            c: 3,
        }
        .to_plain_text(&mut builder);

        println!("{}", builder.build());
    }
}
