use lapin::{
    options::{QueueBindOptions, QueueDeclareOptions},
    types::{AMQPValue, FieldTable},
    Channel, Queue,
};

use crate::{brokers::broker::BrokerConfig, error::BroccoliError};

use super::{broker::RabbitPool, RabbitMQBroker};

impl RabbitMQBroker {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn new_with_config(config: BrokerConfig) -> Self {
        Self {
            pool: None,
            connected: false,
            config: Some(config),
            consume_channels: Default::default(),
        }
    }

    pub(crate) async fn ensure_pool(&self) -> Result<&RabbitPool, BroccoliError> {
        if !self.connected {
            return Err(BroccoliError::Broker(
                "RabbitMQ broker not connected".to_string(),
            ));
        }
        self.pool.as_ref().ok_or_else(|| {
            BroccoliError::Broker("RabbitMQ connection pool not initialized".to_string())
        })
    }

    pub(crate) async fn setup_exchange(
        &self,
        channel: &Channel,
        exchange_name: &str,
    ) -> Result<(), BroccoliError> {
        #[allow(unused_mut)]
        let mut args = FieldTable::default();
        #[cfg(feature = "rabbitmq-delay")]
        args.insert(
            "x-delayed-type".into(),
            AMQPValue::LongString("direct".into()),
        );

        channel
            .exchange_declare(
                exchange_name,
                #[cfg(feature = "rabbitmq-delay")]
                lapin::ExchangeKind::Custom("x-delayed-message".into()),
                #[cfg(not(feature = "rabbitmq-delay"))]
                lapin::ExchangeKind::Direct,
                lapin::options::ExchangeDeclareOptions::default(),
                args.clone(),
            )
            .await
            .map_err(|e| BroccoliError::Broker(format!("Failed to declare exchange: {:?}", e)))?;

        Ok(())
    }

    pub(crate) async fn setup_queue(
        &self,
        channel: &Channel,
        queue_name: &str,
    ) -> Result<Queue, BroccoliError> {
        let mut args = FieldTable::default();
        if !queue_name.contains("failed") {
            args.insert(
                "x-dead-letter-exchange".into(),
                AMQPValue::LongString("".into()),
            );
            args.insert(
                "x-dead-letter-routing-key".into(),
                AMQPValue::LongString(format!("{}_failed", queue_name).into()),
            );
        }

        let queue = channel
            .queue_declare(
                queue_name,
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                args,
            )
            .await
            .map_err(|e| BroccoliError::Broker(format!("Failed to declare queue: {:?}", e)))?;

        channel
            .queue_bind(
                queue_name,
                "broccoli",
                queue_name, // Using queue name as routing key
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| BroccoliError::Broker(format!("Failed to bind queue: {}", e)))?;

        // Setup DLX for failed messages
        let failed_queue = format!("{}_failed", queue_name);
        channel
            .queue_declare(
                &failed_queue,
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| {
                BroccoliError::Broker(format!("Failed to declare failed queue: {:?}", e))
            })?;

        Ok(queue)
    }
}
