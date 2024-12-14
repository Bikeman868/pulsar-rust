use crate::html_builder::{
    ToHtml, HtmlBuilder
};
use pulsar_rust_net::contracts::v1::responses::{
    AckLogEntry, LogEntry, LogEntryDetail, LogEntrySummary, Message, MessageRef, NackLogEntry, PublishLogEntry
};

impl<T> ToHtml<T> for Vec<LogEntry> {
    fn to_html(self: &Self, w: &HtmlBuilder<T>) {
        w.div(self, "log-entries", |w,_: &T,entries|{
            for entry in entries {
                entry.to_html(w);
            }
        });
    }
}

impl<T> ToHtml<T> for Vec<LogEntrySummary> {
    fn to_html(self: &Self, w: &HtmlBuilder<T>) {
        w.div(self, "log-entries", |w,_: &T,entries|{
            for entry in entries {
                entry.to_html(w);
            }
        });
    }
}

impl<T> ToHtml<T> for MessageRef{
    fn to_html(self: &Self, w: &HtmlBuilder<T>) {
        w.div(self, "message-ref", |w,_: &T,mr|{
            w.div(mr, "topic-id", |w,_: &T,mr|{
                w.span(mr, "label topic-id__label", |w,_,_|{
                    w.text("Topic");
                });
                w.span(mr, "field topic-id__id", |w,_,mr|{
                    w.text(&mr.topic_id.to_string());
                });
            });
            w.div(mr, "partition-id", |w,_: &T,mr|{
                w.span(mr, "label partition-id__label", |w,_,_|{
                    w.text("Partition");
                });
                w.span(mr, "field partition-id__id", |w,_,mr|{
                    w.text(&mr.partition_id.to_string());
                });
            });
            w.div(mr, "ledger-id", |w,_: &T,mr|{
                w.span(mr, "label ledger-id__label", |w,_,_|{
                    w.text("Ledger");
                });
                w.span(mr, "field ledger-id__id", |w,_,mr|{
                    w.text(&mr.ledger_id.to_string());
                });
            });
            w.div(mr, "message-id", |w,_: &T,mr|{
                w.span(mr, "label message-id__label", |w,_,_|{
                    w.text("Message");
                });
                w.span(mr, "field message-id__id", |w,_,mr|{
                    w.text(&mr.message_id.to_string());
                });
            });
        });
    }
}

impl<T> ToHtml<T> for Message{
    fn to_html(self: &Self, w: &HtmlBuilder<T>) {
        w.div(self, "message", |w,_: &T,m|{
            w.div(m, "message-metadata", |w,_: &T,m|{
                w.span(m, "label message-metadata__label", |w,_,_|{
                    w.text("Message key");
                });
                w.span(m, "field message-metadata__key", |w,_,m|{
                    w.text(&m.message_key);
                });
            });
            m.message_ref.to_html(w);
            for attribute in &m.attributes {
                w.div(&attribute, "message-attribute", |w,_: &T,attribute|{
                    w.span(attribute, "message-attribute__name", |w,_,attribute|{
                        w.text(&attribute.0);
                    });
                    w.span(&attribute, "message-attribute__value", |w,_,attribute|{
                        w.text(&attribute.1);
                    });
                });
            }
        });
    }
}

impl<T> ToHtml<T> for PublishLogEntry {
    fn to_html(self: &Self, w: &HtmlBuilder<T>) {
        self.message.to_html(w);
    }
}

impl<T> ToHtml<T> for AckLogEntry {
    fn to_html(self: &Self, _w: &HtmlBuilder<T>) {
    }
}

impl<T> ToHtml<T> for NackLogEntry {
    fn to_html(self: &Self, _w: &HtmlBuilder<T>) {
    }
}

impl<T> ToHtml<T> for LogEntryDetail {
    fn to_html(self: &Self, w: &HtmlBuilder<T>) {
        match self {
            LogEntryDetail::Publish(entry) => entry.to_html(w),
            LogEntryDetail::Ack(entry) => entry.to_html(w),
            LogEntryDetail::Nack(entry) => entry.to_html(w),
        }
    }
}

impl<T> ToHtml<T> for LogEntry {
    fn to_html(self: &Self, w: &HtmlBuilder<T>) {
        w.div(self, "log-entry", |w,_: &T,entry|{
            w.div(entry, "event-timestamp", |w,_: &T,entry|{
                w.span(entry, "label event-timestamp__label", |w,_,_|{
                    w.text("Epoc time");
                });
                w.span(entry, "field event-timestamp__value", |w,_,entry|{
                    w.text(&entry.timestamp.to_string());
                });
            });
            w.div(entry, "event-type", |w,_: &T,entry|{
                w.text(&entry.event_type);
            });
            w.div(entry, "event-key", |w,_: &T,entry|{
                w.span(entry, "label event-key__label", |w,_,_|{
                    w.text("Event key");
                });
                w.span(entry, "field event-key__value", |w,_,entry|{
                    w.text(&entry.event_key);
                });
            });
            if let Some(details) = &entry.details {
                details.to_html(w);
            }
        });
    }
}

impl<T> ToHtml<T> for LogEntrySummary {
    fn to_html(self: &Self, w: &HtmlBuilder<T>) {
        w.div(self, "log-entry", |w,_: &T,entry|{
            w.div(entry, "event-timestamp", |w,_: &T,entry|{
                w.span(entry, "label event-timestamp__label", |w,_,_|{
                    w.text("Epoc time");
                });
                w.span(entry, "field event-timestamp__value", |w,_,entry|{
                    w.text(&entry.timestamp.to_string());
                });
            });
            w.div(entry, "event-type", |w,_: &T,entry|{
                w.text(&entry.event_type);
            });
            w.div(entry, "event-key", |w,_: &T,entry|{
                w.span(entry, "label event-key__label", |w,_,_|{
                    w.text("Event key");
                });
                w.span(entry, "field event-key__value", |w,_,entry|{
                    w.text(&entry.event_key);
                });
            });
        });
    }
}

#[cfg(test)]
#[cfg(debug_assertions)]
mod tests {
    use pulsar_rust_net::contracts::v1::responses::{
        AckLogEntry, LogEntry, LogEntryDetail, MessageRef
    };
    use crate::html_builder::{
        ToHtml, HtmlBuilder
    };

    #[test]
    fn should_join_log_entries() {
        let entries: Vec<LogEntry> = vec![
            LogEntry { 
                timestamp: 1, 
                event_type: String::from("Ack"), 
                event_key: String::from("123"), 
                details: None }, 
            LogEntry { 
                timestamp: 2, 
                event_type: String::from("Ack"), 
                event_key: String::from("abc"), 
                details: Some(LogEntryDetail::Ack(AckLogEntry { 
                    message_ref: MessageRef { 
                        topic_id: 5, 
                        partition_id: 2, 
                        ledger_id: 243, 
                        message_id: 98678
                    }, 
                    subscription_id: 3, 
                    consumer_id: 99
                }))
            }];

        let writer = HtmlBuilder::new(entries);

        writer.html(|w,_|{
            w.head("Test", |_,_|{});
            w.body("test", |w,entries|{ entries.to_html(&w); });
        });

        assert_eq!(writer.build(), 
"<!DOCTYPE html><html>
  <head>
    <title>Test</title>
  </head>
  <body class='test'>
    <div class='log-entries'>
      <div class='log-entry'>
        <div class='event-timestamp'>
          <span class='label event-timestamp__label'>Epoc time</span>
          <span class='field event-timestamp__value'>1</span>
        </div>
        <div class='event-type'>
          Ack
        </div>
        <div class='event-key'>
          <span class='label event-key__label'>Event key</span>
          <span class='field event-key__value'>123</span>
        </div>
      </div>
      <div class='log-entry'>
        <div class='event-timestamp'>
          <span class='label event-timestamp__label'>Epoc time</span>
          <span class='field event-timestamp__value'>2</span>
        </div>
        <div class='event-type'>
          Ack
        </div>
        <div class='event-key'>
          <span class='label event-key__label'>Event key</span>
          <span class='field event-key__value'>abc</span>
        </div>
      </div>
    </div>
  </body>
</html>");
}
}
