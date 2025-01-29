#!/bin/bash

# Trap for cleanup
cleanup() {
    echo "Cleaning up containers..."
    docker stop test-redis test-rabbit-mq >/dev/null 2>&1
    docker rm test-redis test-rabbit-mq >/dev/null 2>&1
}
trap cleanup EXIT

run_redis_test() {
    echo "Starting Redis test..."
    docker run --name test-redis -d -p 6380:6379 redis >/dev/null
    sleep 2
    BROCCOLI_QUEUE_URL=redis://localhost:6380 cargo test --features redis
}

run_rabbitmq_test() {
    echo "Starting RabbitMQ test with delay plugin..."
    docker build -f Dockerfile.rabbitmq -t rabbitmq-with-delays .
    docker run --name test-rabbit-mq -d -p 5672:5672 -p 15672:15672 rabbitmq-with-delays >/dev/null
    sleep 5
    BROCCOLI_QUEUE_URL=amqp://localhost:5672 cargo test --features rabbitmq
}


run_redis_bench() {
    echo "Starting Redis benchmark test..."
    docker run --name test-redis -d -p 6380:6379 redis >/dev/null
    sleep 2
    BROCCOLI_QUEUE_URL=redis://localhost:6380 cargo bench --features redis
}

case "$1" in
    "redis") run_redis_test ;;
    "rabbitmq") run_rabbitmq_test ;;
    "redis-bench") run_redis_bench ;;
    *)
        run_redis_test
        cleanup
        run_rabbitmq_test
        ;;
esac


