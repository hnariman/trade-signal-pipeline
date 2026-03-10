use futures_util::StreamExt;
use tokio_tungstenite::tungstenite::Message;

// const BINANCE_MAIN_WSS: &str = "wss://stream.binance.com:9443";
// const BINANCE_FALLBACK_WSS: &str = "wss://stream.binance.com:443";
//
// request format:
// wss://stream.binance.com:9443/ws/btcusdt@trade
//
// response format:
//{
//   "e": "trade",      // event type
//   "E": 1672515782136, // event time (millis)
//   "s": "BTCUSDT",    // symbol
//   "t": 12345,        // trade ID
//   "p": "0.001",      // price (string!)
//   "q": "100",        // quantity (string!)
//   "T": 1672515782136, // trade time (millis)
//   "m": true          // is buyer the market maker?
// }

const KAFKA_BROKERS: &str = "localhost:9092";
const KAFKA_TOPIC: &str = "raw-trades.btc";
const WS_URL: &str = "wss://stream.binance.com:9443/ws/btcusdt@trade";

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "ingester=debug".into()))
        .json()
        .init();

    ensure_topic(KAFKA_BROKERS, KAFKA_TOPIC).await;

    // supervisor loop - tokio-supervisor probably better on prod env
    loop {
        tracing::info!("ingestion pipeline start");
        // slight performance hit to recreate producer on failure, but handshake shall be few ms
        let producer = match rdkafka::ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .create()
        {
            Ok(p) => p,
            Err(e) => {
                // intentional backoff - to prevent spamming Kafka if k8s redeploying/scaling
                tracing::error!(error=%e, "failed to create kafka producer, retrying...");
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                continue;
            }
        };

        // 1000 msg buffer - at ~500 msgs/sec peak BTC volume, ~2s of backpressure before blocking
        let (tx, rx) = tokio::sync::mpsc::channel::<String>(1_000);
        let handle = tokio::spawn(producer_task(rx, producer));

        loop {
            match connect_and_stream(&tx).await {
                Ok(_) => tracing::warn!("stream ended, reconnecting..."),
                Err(e) => tracing::error!("stream error: {}, reconnecting...", e),
            }

            if handle.is_finished() {
                tracing::error!("kafka producer task died - exiting");
                break;
            }
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
        // dead task cleanup timeout
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

async fn connect_and_stream(tx: &tokio::sync::mpsc::Sender<String>) -> anyhow::Result<()> {
    let (ws_stream, _) = tokio_tungstenite::connect_async(WS_URL).await?;
    let (_, mut read) = ws_stream.split();

    while let Some(msg) = read.next().await {
        match msg? {
            Message::Text(text) => {
                if tx.send(text.to_string()).await.is_err() {
                    tracing::warn!("kafka producer task dead, channel closed");
                    break;
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }
    Ok(())
}

async fn ensure_topic(brokers: &str, topic: &str) {
    let admin: rdkafka::admin::AdminClient<_> = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("admin client problem");

    let new_topic =
        rdkafka::admin::NewTopic::new(topic, 3, rdkafka::admin::TopicReplication::Fixed(1));

    let result = admin
        .create_topics(&[new_topic], &rdkafka::admin::AdminOptions::new())
        .await;

    match result {
        Ok(_) => tracing::info!(topic, "topic ready"),
        Err(e) => tracing::error!(error=%e,"ensure topic failed"),
    }
}

async fn producer_task(
    mut rx: tokio::sync::mpsc::Receiver<String>,
    producer: rdkafka::producer::FutureProducer,
) {
    let timeout = rdkafka::util::Timeout::After(std::time::Duration::from_secs(3));
    while let Some(text) = rx.recv().await {
        let key = chrono::Utc::now().timestamp_millis().to_le_bytes();
        let record = rdkafka::producer::FutureRecord::to(KAFKA_TOPIC)
            .payload(text.as_str())
            .key(&key);

        match producer.send(record, timeout).await {
            Ok(v) => tracing::debug!(partition = v.partition, offset = v.offset, "message sent"),
            Err((e, _)) => tracing::error!(error=%e, "kafka produce failed"),
        }
    }
}
