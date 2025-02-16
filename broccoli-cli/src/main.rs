use anyhow::Result;
use broccoli_queue::{
    brokers::management::{QueueStatus, QueueType},
    queue::BroccoliQueue,
};
use clap::{Parser, Subcommand, ValueEnum};
use prettytable::{format, Cell, Row, Table};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Clone, ValueEnum)]
enum QueueTypeCli {
    Failed,
    Processing,
}

impl From<QueueTypeCli> for QueueType {
    fn from(queue_type: QueueTypeCli) -> Self {
        match queue_type {
            QueueTypeCli::Failed => QueueType::Failed,
            QueueTypeCli::Processing => QueueType::Processing,
        }
    }
}

#[derive(Subcommand)]
enum Commands {
    /// Retry messages from failed or processing queues
    Retry {
        /// Queue URL
        #[arg(long, env)]
        broccoli_queue_url: String,
        /// Queue name
        queue_name: String,
        /// Source queue type (failed or processing)
        #[arg(value_enum)]
        source: QueueTypeCli,
    },
    /// Show status of queues
    Status {
        /// Queue URL
        #[arg(long, env)]
        broccoli_queue_url: String,
        /// Queue name. If not provided, all queues will be shown
        queue_name: Option<String>,
    },
}

async fn retry_queue(queue: BroccoliQueue, queue_name: String, source: QueueType) -> Result<()> {
    let count = queue.retry_queue(queue_name, source).await?;
    if count == 0 {
        println!("No messages to retry");
    } else {
        println!("Successfully retried {} messages", count);
    }
    Ok(())
}

async fn show_status(queue: BroccoliQueue, queue_name: Option<String>) -> Result<()> {
    let statuses = queue.queue_status(queue_name).await?;
    print_status_table(&statuses)?;
    Ok(())
}

fn print_status_table(statuses: &[QueueStatus]) -> Result<()> {
    let mut table = Table::new();

    table.set_format(*format::consts::FORMAT_BOX_CHARS);

    table.set_format(*format::consts::FORMAT_BOX_CHARS);

    table.add_row(Row::new(vec![
        Cell::new("Queue").style_spec("Fb"),
        Cell::new("Type").style_spec("Fb"),
        Cell::new("Main Queue Size").style_spec("Fb"),
        Cell::new("Processing Queue Size").style_spec("Fb"),
        Cell::new("Failed Queue Size").style_spec("Fb"),
        Cell::new("Disambiguators").style_spec("Fb"),
    ]));

    for status in statuses {
        table.add_row(Row::new(vec![
            Cell::new(&status.name),
            Cell::new(&format!("{:?}", status.queue_type)),
            Cell::new(&status.size.to_string()),
            Cell::new(&status.processing.to_string()),
            Cell::new(&status.failed.to_string()),
            Cell::new(&status.disambiguator_count.unwrap_or(0).to_string()),
        ]));
    }

    table.printstd();

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let cli = Cli::parse();

    match cli.command {
        Commands::Retry {
            broccoli_queue_url,
            queue_name,
            source,
        } => {
            let queue = BroccoliQueue::builder(broccoli_queue_url).build().await?;
            retry_queue(queue, queue_name, source.into()).await?;
        }
        Commands::Status {
            broccoli_queue_url,
            queue_name,
        } => {
            let queue = BroccoliQueue::builder(broccoli_queue_url).build().await?;
            show_status(queue, queue_name).await?;
        }
    }

    Ok(())
}
