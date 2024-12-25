use crate::formatting::html_builder::{HtmlBuilder, ToHtml};
use pulsar_rust_net::contracts::v1::responses::{
    AckLogEntry, DropConsumerLogEntry, KeyAffinityLogEntry, LogEntry, LogEntryDetail,
    LogEntrySummary, Message, MessageRef, NackLogEntry, NewConsumerLogEntry, PublishLogEntry,
};

impl<T> ToHtml<T> for Vec<LogEntry> {
    fn to_html(self: &Self, w: &HtmlBuilder<T>) {
        w.div(self, "log-entries", |w, _: &T, entries| {
            for entry in entries {
                entry.to_html(w);
            }
        });
    }
}

impl<T> ToHtml<T> for Vec<LogEntrySummary> {
    fn to_html(self: &Self, w: &HtmlBuilder<T>) {
        w.div(self, "log-entries", |w, _: &T, entries| {
            for entry in entries {
                entry.to_html(w);
            }
        });
    }
}

impl<T> ToHtml<T> for MessageRef {
    fn to_html(self: &Self, w: &HtmlBuilder<T>) {
        w.div(self, "message-ref", |w, _: &T, mr| {
            w.div(mr, "topic-id", |w, _: &T, mr| {
                w.span(mr, "label topic-id__label", |w, _, _| {
                    w.text("Topic");
                });
                w.span(mr, "field topic-id__id", |w, _, mr| {
                    w.text(&mr.topic_id.to_string());
                });
            });
            w.div(mr, "partition-id", |w, _: &T, mr| {
                w.span(mr, "label partition-id__label", |w, _, _| {
                    w.text("Partition");
                });
                w.span(mr, "field partition-id__id", |w, _, mr| {
                    w.text(&mr.partition_id.to_string());
                });
            });
            w.div(mr, "ledger-id", |w, _: &T, mr| {
                w.span(mr, "label ledger-id__label", |w, _, _| {
                    w.text("Ledger");
                });
                w.span(mr, "field ledger-id__id", |w, _, mr| {
                    w.text(&mr.ledger_id.to_string());
                });
            });
            w.div(mr, "message-id", |w, _: &T, mr| {
                w.span(mr, "label message-id__label", |w, _, _| {
                    w.text("Message");
                });
                w.span(mr, "field message-id__id", |w, _, mr| {
                    w.text(&mr.message_id.to_string());
                });
            });
        });
    }
}

impl<T> ToHtml<T> for Message {
    fn to_html(self: &Self, w: &HtmlBuilder<T>) {
        w.div(self, "message", |w, _: &T, m| {
            w.div(m, "message-metadata", |w, _: &T, m| {
                w.span(m, "label message-metadata__label", |w, _, _| {
                    w.text("Message key");
                });
                w.span(m, "field message-metadata__key", |w, _, m| {
                    w.text(&m.message_key);
                });
            });
            m.message_ref.to_html(w);
            for attribute in &m.attributes {
                w.div(&attribute, "message-attribute", |w, _: &T, attribute| {
                    w.span(attribute, "message-attribute__name", |w, _, attribute| {
                        w.text(&attribute.0);
                    });
                    w.span(&attribute, "message-attribute__value", |w, _, attribute| {
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
    fn to_html(self: &Self, w: &HtmlBuilder<T>) {
        w.div(self, "ack", |w, _: &T, ack| {
            w.div(ack, "subscription-id", |w, _: &T, ack| {
                w.span(ack, "label subscription-id__label", |w, _, _| {
                    w.text("Subscription");
                });
                w.span(ack, "field subscription-id__id", |w, _, ack| {
                    w.text(&ack.subscription_id.to_string());
                });
            });
            w.div(ack, "consumer-id", |w, _: &T, ack| {
                w.span(ack, "label consumer-id__label", |w, _, _| {
                    w.text("Consumer");
                });
                w.span(ack, "field consumer-id__id", |w, _, ack| {
                    w.text(&ack.consumer_id.to_string());
                });
            });
            ack.message_ref.to_html(w);
        });
    }
}

impl<T> ToHtml<T> for NackLogEntry {
    fn to_html(self: &Self, w: &HtmlBuilder<T>) {
        w.div(self, "nack", |w, _: &T, nack| {
            w.div(nack, "subscription-id", |w, _: &T, nack| {
                w.span(nack, "label subscription-id__label", |w, _, _| {
                    w.text("Subscription");
                });
                w.span(nack, "field subscription-id__id", |w, _, nack| {
                    w.text(&nack.subscription_id.to_string());
                });
            });
            w.div(nack, "consumer-id", |w, _: &T, nack| {
                w.span(nack, "label consumer-id__label", |w, _, _| {
                    w.text("Consumer");
                });
                w.span(nack, "field consumer-id__id", |w, _, nack| {
                    w.text(&nack.consumer_id.to_string());
                });
            });
            nack.message_ref.to_html(w);
        });
    }
}

impl<T> ToHtml<T> for NewConsumerLogEntry {
    fn to_html(self: &Self, w: &HtmlBuilder<T>) {
        w.div(self, "new-consumer", |w, _: &T, c| {
            w.div(c, "topic-id", |w, _: &T, c| {
                w.span(c, "label topic-id__label", |w, _, _| {
                    w.text("Topic");
                });
                w.span(c, "field topic-id__id", |w, _, c| {
                    w.text(&c.topic_id.to_string());
                });
            });
            w.div(c, "subscription-id", |w, _: &T, c| {
                w.span(c, "label subscription-id__label", |w, _, _| {
                    w.text("Subscription");
                });
                w.span(c, "field subscription-id__id", |w, _, c| {
                    w.text(&c.subscription_id.to_string());
                });
            });
            w.div(c, "consumer-id", |w, _: &T, c| {
                w.span(c, "label consumer-id__label", |w, _, _| {
                    w.text("Consumer");
                });
                w.span(c, "field consumer-id__id", |w, _, c| {
                    w.text(&c.consumer_id.to_string());
                });
            });
        });
    }
}

impl<T> ToHtml<T> for DropConsumerLogEntry {
    fn to_html(self: &Self, w: &HtmlBuilder<T>) {
        w.div(self, "drop-consumer", |w, _: &T, c| {
            w.div(c, "topic-id", |w, _: &T, c| {
                w.span(c, "label topic-id__label", |w, _, _| {
                    w.text("Topic");
                });
                w.span(c, "field topic-id__id", |w, _, c| {
                    w.text(&c.topic_id.to_string());
                });
            });
            w.div(c, "subscription-id", |w, _: &T, c| {
                w.span(c, "label subscription-id__label", |w, _, _| {
                    w.text("Subscription");
                });
                w.span(c, "field subscription-id__id", |w, _, c| {
                    w.text(&c.subscription_id.to_string());
                });
            });
            w.div(c, "consumer-id", |w, _: &T, c| {
                w.span(c, "label consumer-id__label", |w, _, _| {
                    w.text("Consumer");
                });
                w.span(c, "field consumer-id__id", |w, _, c| {
                    w.text(&c.consumer_id.to_string());
                });
            });
        });
    }
}

impl<T> ToHtml<T> for KeyAffinityLogEntry {
    fn to_html(self: &Self, w: &HtmlBuilder<T>) {
        w.div(self, "key-affinity", |w, _: &T, c| {
            w.div(c, "topic-id", |w, _: &T, c| {
                w.span(c, "label topic-id__label", |w, _, _| {
                    w.text("Topic");
                });
                w.span(c, "field topic-id__id", |w, _, c| {
                    w.text(&c.topic_id.to_string());
                });
            });
            w.div(c, "subscription-id", |w, _: &T, c| {
                w.span(c, "label subscription-id__label", |w, _, _| {
                    w.text("Subscription");
                });
                w.span(c, "field subscription-id__id", |w, _, c| {
                    w.text(&c.subscription_id.to_string());
                });
            });
            w.div(c, "consumer-id", |w, _: &T, c| {
                w.span(c, "label consumer-id__label", |w, _, _| {
                    w.text("Consumer");
                });
                w.span(c, "field consumer-id__id", |w, _, c| {
                    w.text(&c.consumer_id.to_string());
                });
            });
            w.div(c, "message-key", |w, _: &T, c| {
                w.span(c, "label message-key__label", |w, _, _| {
                    w.text("Message key");
                });
                w.span(c, "field message-key__key", |w, _, c| {
                    w.text(&c.message_key);
                });
            });
        });
    }
}

impl<T> ToHtml<T> for LogEntryDetail {
    fn to_html(self: &Self, w: &HtmlBuilder<T>) {
        match self {
            LogEntryDetail::Publish(entry) => entry.to_html(w),
            LogEntryDetail::Ack(entry) => entry.to_html(w),
            LogEntryDetail::Nack(entry) => entry.to_html(w),
            LogEntryDetail::NewConsumer(entry) => entry.to_html(w),
            LogEntryDetail::DropConsumer(entry) => entry.to_html(w),
            LogEntryDetail::KeyAffinity(entry) => entry.to_html(w),
        }
    }
}

impl<T> ToHtml<T> for LogEntry {
    fn to_html(self: &Self, w: &HtmlBuilder<T>) {
        w.div(self, "log-entry", |w, _: &T, entry| {
            w.div(entry, "event-header", |w, _: &T, entry| {
                w.div(entry, "event-timestamp", |w, _: &T, entry| {
                    w.span(entry, "label event-timestamp__label", |w, _, _| {
                        w.text("Epoc time");
                    });
                    w.span(entry, "field event-timestamp__value", |w, _, entry| {
                        w.text(&entry.timestamp.to_string());
                    });
                });
                w.div(entry, "event-type", |w, _: &T, entry| {
                    w.text(&entry.event_type);
                });
                w.div(entry, "event-key", |w, _: &T, entry| {
                    w.span(entry, "label event-key__label", |w, _, _| {
                        w.text("Event key");
                    });
                    w.span(entry, "field event-key__value", |w, _, entry| {
                        w.text(&entry.event_key);
                    });
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
        w.div(self, "log-entry", |w, _: &T, entry| {
            w.div(entry, "event-timestamp", |w, _: &T, entry| {
                w.span(entry, "label event-timestamp__label", |w, _, _| {
                    w.text("Epoc time");
                });
                w.span(entry, "field event-timestamp__value", |w, _, entry| {
                    w.text(&entry.timestamp.to_string());
                });
            });
            w.div(entry, "event-type", |w, _: &T, entry| {
                w.text(&entry.event_type);
            });
            w.div(entry, "event-key", |w, _: &T, entry| {
                w.span(entry, "label event-key__label", |w, _, _| {
                    w.text("Event key");
                });
                w.span(entry, "field event-key__value", |w, _, entry| {
                    w.text(&entry.event_key);
                });
            });
        });
    }
}

#[cfg(test)]
#[cfg(debug_assertions)]
mod tests {
    use crate::formatting::html_builder::{HtmlBuilder, ToHtml};
    use pulsar_rust_net::contracts::v1::responses::{
        AckLogEntry, LogEntry, LogEntryDetail, MessageRef,
    };

    #[test]
    fn should_join_log_entries() {
        let entries: Vec<LogEntry> = vec![
            LogEntry {
                timestamp: 1,
                event_type: String::from("Ack"),
                event_key: String::from("123"),
                details: None,
            },
            LogEntry {
                timestamp: 2,
                event_type: String::from("Ack"),
                event_key: String::from("abc"),
                details: Some(LogEntryDetail::Ack(AckLogEntry {
                    message_ref: MessageRef {
                        topic_id: 5,
                        partition_id: 2,
                        ledger_id: 243,
                        message_id: 98678,
                    },
                    subscription_id: 3,
                    consumer_id: 99,
                })),
            },
        ];

        let writer = HtmlBuilder::new(entries);

        writer.html(|w, _| {
            w.head("Test", |_, _| {});
            w.body("test", |w, entries| {
                entries.to_html(&w);
            });
        });

        println!("{}", writer.build());
    }
}
