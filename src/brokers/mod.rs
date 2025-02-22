/// Contains the generic interfaces for brokers
pub mod broker;
/// Contains functions to connect to a broker
pub(crate) mod connect;
/// Contains the `RabbitMQ` broker implementation
#[cfg(feature = "rabbitmq")]
pub mod rabbitmq;
/// Contains the Redis broker implementation
#[cfg(feature = "redis")]
pub mod redis;
/// Contains the SurrealDB broker implementation
#[cfg(feature = "surrealdb")]
pub mod surrealdb;
