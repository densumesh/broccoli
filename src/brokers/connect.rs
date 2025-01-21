use crate::{
    brokers::broker::{Broker, BrokerConfig, BrokerType},
    error::BroccoliError,
};

#[cfg(feature = "rabbitmq")]
use crate::brokers::rabbitmq::RabbitMQBroker;
#[cfg(feature = "redis")]
use crate::brokers::redis::RedisBroker;

/// Returns a boxed broker implementation based on the broker URL.
///
/// # Arguments
/// * `broker_url` - The URL of the broker.
/// * `config` - Optional broker configuration.
///
/// # Returns
/// A `Result` containing a boxed broker implementation, or a `BroccoliError` on failure.
pub(crate) async fn connect_to_broker(
    broker_url: &str,
    config: Option<BrokerConfig>,
) -> Result<Box<dyn Broker>, BroccoliError> {
    #[cfg(all(feature = "redis", feature = "rabbitmq"))]
    let broker_type = if broker_url.starts_with("redis") {
        BrokerType::Redis
    } else if broker_url.starts_with("amqp") {
        BrokerType::RabbitMQ
    } else {
        return Err(BroccoliError::Broker(
            "Unsupported broker URL scheme".to_string(),
        ));
    };

    #[cfg(all(feature = "redis", not(feature = "rabbitmq")))]
    let broker_type = if broker_url.starts_with("redis") {
        BrokerType::Redis
    } else {
        return Err(BroccoliError::Broker(
            "Unsupported broker URL scheme".to_string(),
        ));
    };

    #[cfg(all(not(feature = "redis"), feature = "rabbitmq"))]
    let broker_type = if broker_url.starts_with("amqp") {
        BrokerType::RabbitMQ
    } else {
        return Err(BroccoliError::Broker(
            "Unsupported broker URL scheme".to_string(),
        ));
    };

    let mut broker: Box<dyn Broker> = match broker_type {
        #[cfg(feature = "redis")]
        BrokerType::Redis => Box::new(match config {
            Some(config) => RedisBroker::new_with_config(config),
            None => RedisBroker::new(),
        }),
        #[cfg(feature = "rabbitmq")]
        BrokerType::RabbitMQ => Box::new(match config {
            Some(config) => RabbitMQBroker::new_with_config(config),
            None => RabbitMQBroker::new(),
        }),
    };

    broker.connect(broker_url).await?;

    Ok(broker)
}
