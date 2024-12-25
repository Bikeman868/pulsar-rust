pub trait JoinableToString {
    fn join(&self, sep: &str) -> String;
}
