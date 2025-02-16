pub mod errors {
    use thiserror::Error;

    #[derive(Error, Debug)]
    pub enum CliError {
        #[error("Redis error: {0}")]
        Redis(#[from] redis::RedisError),
        #[error("Environment error: {0}")]
        Env(#[from] std::env::VarError),
        #[error("IO error: {0}")]
        Io(#[from] std::io::Error),
    }
}

pub use errors::CliError;
