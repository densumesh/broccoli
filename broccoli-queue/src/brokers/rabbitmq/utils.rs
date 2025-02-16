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

    pub(crate) fn ensure_pool(&self) -> Result<&RabbitPool, BroccoliError> {
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
        let exchange_kind = if self
            .config
            .as_ref()
            .is_some_and(|c| c.enable_scheduling.unwrap_or(false))
        {
            args.insert(
                "x-delayed-type".into(),
                AMQPValue::LongString("direct".into()),
            );
            lapin::ExchangeKind::Custom("x-delayed-message".into())
        } else {
            lapin::ExchangeKind::Direct
        };

        channel
            .exchange_declare(
                exchange_name,
                exchange_kind,
                lapin::options::ExchangeDeclareOptions::default(),
                args.clone(),
            )
            .await
            .map_err(|e| BroccoliError::Broker(format!("Failed to declare exchange: {e:?}")))?;

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
                AMQPValue::LongString(format!("{queue_name}_failed").into()),
            );
        }

        args.insert("x-max-priority".into(), AMQPValue::LongInt(5));

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
            .map_err(|e| BroccoliError::Broker(format!("Failed to declare queue: {e:?}")))?;

        channel
            .queue_bind(
                queue_name,
                "broccoli",
                queue_name, // Using queue name as routing key
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| BroccoliError::Broker(format!("Failed to bind queue: {e}")))?;

        // Setup DLX for failed messages
        let failed_queue = format!("{queue_name}_failed");
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
            .map_err(|e| BroccoliError::Broker(format!("Failed to declare failed queue: {e:?}")))?;

        Ok(queue)
    }
}
