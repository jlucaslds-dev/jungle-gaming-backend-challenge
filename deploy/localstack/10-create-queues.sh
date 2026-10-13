#!/bin/bash
set -eu

awslocal sqs get-queue-url --queue-name wager-transactions-dlq.fifo >/dev/null 2>&1 || \
  awslocal sqs create-queue \
    --queue-name wager-transactions-dlq.fifo \
    --attributes '{"FifoQueue":"true","ContentBasedDeduplication":"false"}'

DLQ_ARN="$(awslocal sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/wager-transactions-dlq.fifo \
  --attribute-names QueueArn \
  --query 'Attributes.QueueArn' \
  --output text)"
REDRIVE="{\\\"deadLetterTargetArn\\\":\\\"${DLQ_ARN}\\\",\\\"maxReceiveCount\\\":\\\"5\\\"}"

awslocal sqs get-queue-url --queue-name wager-transactions.fifo >/dev/null 2>&1 || \
  awslocal sqs create-queue \
    --queue-name wager-transactions.fifo \
    --attributes "{\"FifoQueue\":\"true\",\"ContentBasedDeduplication\":\"false\",\"VisibilityTimeout\":\"60\",\"ReceiveMessageWaitTimeSeconds\":\"20\",\"RedrivePolicy\":\"${REDRIVE}\"}"

awslocal sqs get-queue-url --queue-name outbox-events.fifo >/dev/null 2>&1 || \
  awslocal sqs create-queue \
    --queue-name outbox-events.fifo \
    --attributes '{"FifoQueue":"true","ContentBasedDeduplication":"false","VisibilityTimeout":"60"}'
