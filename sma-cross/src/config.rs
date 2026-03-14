pub struct Config {
    pub brokers: String,
    pub consumer_group: String,
    pub topic_normalized: String,
    pub topic_signals: String,
    pub fast_period: usize,
    pub slow_period: usize,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            brokers: std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into()),
            consumer_group: std::env::var("CONSUMER_GROUP")
                .unwrap_or_else(|_| "sma-cross-v1".into()),
            topic_normalized: std::env::var("KAFKA_TOPIC_NORMALIZED")
                .unwrap_or_else(|_| "normalized-trades.btc".into()),
            topic_signals: std::env::var("KAFKA_TOPIC_SIGNALS")
                .unwrap_or_else(|_| "signals.btc".into()),
            fast_period: std::env::var("FAST_PERIOD")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(5),
            slow_period: std::env::var("SLOW_PERIOD")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(20),
        }
    }
}
