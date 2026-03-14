mod config;
mod sma;
use futures_util::StreamExt;
use rdkafka::{consumer::Consumer, Message};
use rust_decimal::Decimal;
use sma::{CrossSignal, SmaState};

#[derive(serde::Deserialize)]
struct NormalizedTrade {
    symbol: String,
    price: Decimal,
    trade_time: u64,
}

#[derive(serde::Serialize)]
struct SignalEvent {
    symbol: String,
    signal: String,
    fast_sma: Decimal,
    slow_sma: Decimal,
    price: Decimal,
    timestamp_ms: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = config::Config::from_env();
    tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "analyzer=info".into()))
        .json()
        .init();

    let producer: rdkafka::producer::FutureProducer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", &config.brokers)
        .create()?;

    let consumer: rdkafka::consumer::StreamConsumer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", &config.brokers)
        .set("group.id", &config.consumer_group)
        .set("auto.offset.reset", "latest")
        .create()?;

    consumer.subscribe(&[&config.topic_normalized])?;

    let mut sma = SmaState::new(config.fast_period, config.slow_period);

    tracing::info!(
        fast = config.fast_period,
        slow = config.slow_period,
        "analyzer started"
    );

    while let Some(msg) = consumer.stream().next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                tracing::error!(error=%e, "kafka error");
                continue;
            }
        };

        let payload = match msg.payload_view::<str>() {
            Some(Ok(s)) => s,
            _ => continue,
        };

        let trade: NormalizedTrade = match sonic_rs::from_str(payload) {
            Ok(t) => t,
            Err(e) => {
                tracing::error!(error=%e, "parse error");
                continue;
            }
        };

        if let Some(cross) = sma.update(trade.price) {
            let signal_type = match cross {
                CrossSignal::Above => "cross_above",
                CrossSignal::Below => "cross_below",
            };

            let event = SignalEvent {
                symbol: trade.symbol.clone(),
                signal: signal_type.to_string(),
                fast_sma: sma.fast_sma().unwrap(),
                slow_sma: sma.slow_sma().unwrap(),
                price: trade.price,
                timestamp_ms: trade.trade_time,
            };

            tracing::info!(
                symbol = %event.symbol,
                signal = %event.signal,
                price  = %event.price,
                fast_sma = %event.fast_sma,
                slow_sma = %event.slow_sma,
                "SIGNAL FIRED"
            );

            let payload = sonic_rs::to_string(&event).unwrap_or_default();
            let record = rdkafka::producer::FutureRecord::to(&config.topic_signals)
                .payload(&payload)
                .key(&trade.symbol);
            let timeout = rdkafka::util::Timeout::After(std::time::Duration::from_secs(2));
            let _ = producer.send(record, timeout).await;
        }
    }

    Ok(())
}
