#!/bin/bash

# Start Redis server using Docker
echo "Starting Redis server using Docker..."
docker run --name test-redis -d -p 6380:6379 redis > /dev/null

# Wait for Redis server to start
sleep 2


# Run cargo tests
echo "Running cargo tests..."
BROCCOLI_QUEUE_URL=redis://localhost:6380 cargo test --features redis

# Stop and remove Redis container
echo "Stopping Redis server..."
docker stop test-redis > /dev/null
docker rm test-redis > /dev/null

# Build the custom image
echo "Building custom RabbitMQ image with plugins..."
docker build -f Dockerfile.rabbitmq -t rabbitmq-with-delays .

# Run the container with the custom image
echo "Starting RabbitMQ server with delayed message exchange plugin using Docker..."
docker run --name test-rabbit-mq -d -p 5672:5672 -p 15672:15672 rabbitmq-with-delays > /dev/null

# Wait for RabbitMQ server to start and plugin to be enabled
echo "Waiting for RabbitMQ to start..."
sleep 5

# Run cargo tests
echo "Running cargo tests..."
BROCCOLI_QUEUE_URL=amqp://localhost:5672 cargo test --features rabbitmq,rabbitmq-delay

# Stop and remove RabbitMQ container
echo "Cleaning up..."
docker stop test-rabbit-mq > /dev/null
docker rm test-rabbit-mq > /dev/null


# Run the container with the custom image
echo "Starting RabbitMQ server without the plugins..."
docker run --name test-rabbit-mq -d -p 5672:5672 -p 15672:15672 rabbitmq > /dev/null

# Wait for RabbitMQ server to start and plugin to be enabled
echo "Waiting for RabbitMQ to start..."
sleep 5

# Run cargo tests
echo "Running cargo tests..."
BROCCOLI_QUEUE_URL=amqp://localhost:5672 cargo test --features rabbitmq

# Stop and remove RabbitMQ container
echo "Cleaning up..."
docker stop test-rabbit-mq > /dev/null
docker rm test-rabbit-mq > /dev/null