use crate::{
    brokers::broker::{Broker, BrokerConfig, BrokerType},
    error::BroccoliError,
};

#[cfg(feature = "management")]
use crate::brokers::management::BrokerWithManagement;

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
#[cfg(not(feature = "management"))]
pub async fn connect_to_broker(
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
        BrokerType::Redis => Box::new(config.map_or_else(RedisBroker::new, |config| {
            RedisBroker::new_with_config(config)
        })),
        #[cfg(feature = "rabbitmq")]
        BrokerType::RabbitMQ => Box::new(config.map_or_else(RabbitMQBroker::new, |config| {
            RabbitMQBroker::new_with_config(config)
        })),
    };

    broker.connect(broker_url).await?;

    Ok(broker)
}

/// Returns a boxed broker implementation with management capabilities based on the broker URL.
///
/// # Arguments
/// * `broker_url` - The URL of the broker.
/// * `config` - Optional broker configuration.
///
/// # Returns
/// A `Result` containing a boxed broker implementation with management capabilities, or a `BroccoliError` on failure.
#[cfg(feature = "management")]
pub async fn connect_to_broker(
    broker_url: &str,
    config: Option<BrokerConfig>,
) -> Result<Box<dyn BrokerWithManagement>, BroccoliError> {
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

    let mut broker: Box<dyn BrokerWithManagement> = match broker_type {
        #[cfg(feature = "redis")]
        BrokerType::Redis => Box::new(config.map_or_else(RedisBroker::new, |config| {
            RedisBroker::new_with_config(config)
        })),
        #[cfg(feature = "rabbitmq")]
        BrokerType::RabbitMQ => Box::new(config.map_or_else(RabbitMQBroker::new, |config| {
            RabbitMQBroker::new_with_config(config)
        })),
    };

    broker.connect(broker_url).await?;

    Ok(broker)
}
