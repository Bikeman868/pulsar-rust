use std::cell::RefCell;
use crate::build_number::BUILD_NUMBER;

pub trait ToHtml<T> {
    fn to_html(self: &Self, w: &HtmlBuilder<T>);
}

struct HtmlBuilderMutableState {
    buffer: String,
    indent: usize,
    line_break: bool,
}

pub struct HtmlBuilder<T> {
    mutable: RefCell<HtmlBuilderMutableState>,
    document: T,
}

impl<TDoc> HtmlBuilder<TDoc> {
    fn write(self: &Self, str: &str) {
        let mut mutable = self.mutable.borrow_mut();
        if mutable.line_break { 
            mutable.buffer.push('\n');
            for _ in 0..mutable.indent { mutable.buffer.push_str("  ") };
            mutable.line_break = false;
        }
        mutable.buffer.push_str(str);
    }

    #[cfg(debug_assertions)]
    fn indent(self: &Self) {
        let mut mutable = self.mutable.borrow_mut();
        mutable.indent += 1;
        mutable.line_break = true;
    }

    #[cfg(debug_assertions)]
    fn outdent(self: &Self) {
        let mut mutable = self.mutable.borrow_mut();
        mutable.indent -= 1;
        mutable.line_break = true;
    }

    #[cfg(debug_assertions)]
    fn line_break(self: &Self) {
        let mut mutable = self.mutable.borrow_mut();
        mutable.line_break = true;
    }

    #[cfg(not(debug_assertions))]
    #[inline]
    fn indent(self: &Self) {}

    #[cfg(not(debug_assertions))]
    #[inline]
    fn outdent(self: &Self) {}

    #[cfg(not(debug_assertions))]
    #[inline]
    fn line_break(self: &Self) {}

    pub fn new (document: TDoc) -> Self {
        Self { 
            document,
            mutable: RefCell::new(HtmlBuilderMutableState{
                buffer: String::with_capacity(4096),
                indent: 0,
                line_break: false,
            }),
        }
    }

    pub fn build(self: Self) -> String { self.mutable.into_inner().buffer }

    pub fn text(self: &Self, text: &str) { self.write(text) }

    pub fn html(self: &Self, f: fn(&Self, &TDoc)) {
        self.write("<!DOCTYPE html><html>");
        self.indent();
        f(self, &self.document);
        self.outdent();
        self.write("</html>");
    }

    pub fn head(self: &Self, title: &str, f: fn(&Self, &TDoc)) {
        self.write("<head>");
        self.indent();
        self.write("<title>");
        self.write(title);
        self.write("</title>");
        self.line_break();
        f(self, &self.document);
        self.outdent();
        self.write("</head>");
        self.line_break();
    }

    pub fn css(self: &Self, uri: &str) {
        self.write("<link rel='stylesheet' type='text/css' href='");
        self.write(uri);
        self.write("?build=");
        self.write(BUILD_NUMBER);
        self.write("'>");
        self.line_break();
    }

    pub fn body(self: &Self, class: &str, f: fn(&Self, &TDoc)) {
        self.write("<body class='");
        self.write(class);
        self.write("'>");
        self.indent();
        f(self, &self.document);
        self.outdent();
        self.write("</body>");
    }

    pub fn h1(self: &Self, class: &str, text: &str) {
        if class.len() > 0 {
            self.write("<h1 class='");
            self.write(class);
            self.write("'>");
        } else {
            self.write("<h1>");
        }
        self.write(text);
        self.write("</h1>");
    }

    pub fn h2(self: &Self, class: &str, text: &str) {
        if class.len() > 0 {
            self.write("<h2 class='");
            self.write(class);
            self.write("'>");
        } else {
            self.write("<h2>");
        }
        self.write(text);
        self.write("</h2>");
    }

    pub fn h3(self: &Self, class: &str, text: &str) {
        if class.len() > 0 {
            self.write("<h3 class='");
            self.write(class);
            self.write("'>");
        } else {
            self.write("<h3>");
        }
        self.write(text);
        self.write("</h3>");
    }

    pub fn span<T>(self: &Self, t: &T, class: &str, f: fn(&Self, &TDoc, &T)) {
        self.write("<span class='");
        self.write(class);
        self.write("'>");
        f(self, &self.document, t);
        self.write("</span>");
        self.line_break();
    }

    pub fn div<T>(self: &Self, t: &T, class: &str, f: fn(&Self, &TDoc, &T)) {
        self.write("<div class='");
        self.write(class);
        self.write("'>");
        self.indent();
        f(self, &self.document, t);
        self.outdent();
        self.write("</div>");
        self.line_break();
    }
}

#[cfg(test)]
#[cfg(debug_assertions)]
mod tests {
    use super::*;

    #[test]
    fn should_render_html() {
        let writer = HtmlBuilder::new(());
        writer.html(|w,_| {
            w.head("Hello", |_,_|());
            w.body("log", |w,_|{
                w.span(&(), "id", |w,_,_|{
                    w.text("Hello")
                })
            })
        });
        let html = writer.build();
        assert_eq!(html, 
"<!DOCTYPE html><html>
  <head>
    <title>Hello</title>
  </head>
  <body class='log'>
    <span class='id'>Hello</span>
  </body>
</html>")
    }
}
