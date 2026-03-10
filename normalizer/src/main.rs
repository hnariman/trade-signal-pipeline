use futures_util::StreamExt;
use rdkafka::{Message, consumer::Consumer};

#[derive(serde::Deserialize)]
struct BinanceRawTrade {
    #[serde(rename = "E")]
    event_time: u64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "t")]
    id: u64,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "q")]
    quantity: String,
    #[serde(rename = "T")]
    trade_time: u64,
    #[serde(rename = "m")]
    buyer_is_market_maker: bool,
}
#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct NormalizedTrade {
    event_time: u64,
    symbol: String,
    id: u64,
    price: rust_decimal::Decimal,
    quantity: rust_decimal::Decimal,
    trade_time: u64,
    buyer_is_market_maker: bool,
}

impl TryFrom<BinanceRawTrade> for NormalizedTrade {
    type Error = anyhow::Error;

    fn try_from(raw: BinanceRawTrade) -> Result<Self, Self::Error> {
        Ok(Self {
            event_time: raw.event_time,
            symbol: raw.symbol,
            id: raw.id,
            price: raw.price.parse()?,
            quantity: raw.quantity.parse()?,
            trade_time: raw.trade_time,
            buyer_is_market_maker: raw.buyer_is_market_maker,
        })
    }
}

impl TryFrom<&str> for NormalizedTrade {
    type Error = anyhow::Error;

    fn try_from(raw: &str) -> Result<Self, Self::Error> {
        let binance_raw: BinanceRawTrade = sonic_rs::from_str(raw)?;
        NormalizedTrade::try_from(binance_raw)
    }
}

const KAFKA_BROKERS: &str = "localhost:9092";
const KAFKA_TOPIC_RAW: &str = "raw-trades.btc";
const KAFKA_TOPIC_NORMALIZED: &str = "normalized-trades.btc";
const CONSUMER_GROUP: &str = "normalizer-v1";
const BOOTSTRAP_SERVER: &str = "bootstrap.servers";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "normalizer=debug".into()))
        .json()
        .init();

    let producer: rdkafka::producer::FutureProducer = rdkafka::ClientConfig::new()
        .set(BOOTSTRAP_SERVER, KAFKA_BROKERS)
        .create()?;
    let consumer: rdkafka::consumer::StreamConsumer = rdkafka::ClientConfig::new()
        .set(BOOTSTRAP_SERVER, KAFKA_BROKERS)
        .set("group.id", CONSUMER_GROUP)
        .set("auto.offset.reset", "earliest")
        .create()?;

    consumer.subscribe(&[KAFKA_TOPIC_RAW])?;

    while let Some(data) = consumer.stream().next().await {
        match data {
            Err(e) => tracing::error!(error=%e,"error"),
            Ok(d) => {
                let payload = match d.payload_view::<str>() {
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        tracing::error!(error=%e,"problem with payload");
                        continue;
                    }
                    None => {
                        tracing::error!("empty payload");
                        continue;
                    }
                };

                match NormalizedTrade::try_from(payload) {
                    Ok(trade) => {
                        tracing::info!(
                            symbol=%trade.symbol,
                            price=%trade.price,
                            quantity=%trade.quantity,
                            "trade normalized"
                        );
                        let payload = sonic_rs::to_string(&trade)?;
                        let key = trade.id.to_string();
                        let record = rdkafka::producer::FutureRecord::to(KAFKA_TOPIC_NORMALIZED)
                            .payload(&payload)
                            .key(&key);

                        let timeout =
                            rdkafka::util::Timeout::After(std::time::Duration::from_secs(2));

                        match producer.send(record, timeout).await {
                            Ok(v) => {
                                tracing::info!(
                                    partition = v.partition,
                                    offset = v.offset,
                                    "added the information"
                                )
                            }
                            Err((e, _)) => {
                                tracing::error!(error=%e,"unable to send normalized data to topic")
                            }
                        };
                    }
                    Err(e) => {
                        tracing::error!(error=%e,"normalization error");
                    }
                }
            }
        }
    }

    Ok(())
}
