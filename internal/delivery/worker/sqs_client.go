package worker

import (
	"context"
	"errors"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type SQSAPI interface {
	ReceiveMessage(context.Context, *sqs.ReceiveMessageInput, ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(context.Context, *sqs.DeleteMessageInput, ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(context.Context, *sqs.ChangeMessageVisibilityInput, ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
	SendMessage(context.Context, *sqs.SendMessageInput, ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	GetQueueAttributes(context.Context, *sqs.GetQueueAttributesInput, ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error)
}

type Client struct {
	API            SQSAPI
	WagerQueueURL  string
	OutboxQueueURL string
}

func NewClient(ctx context.Context) (*Client, error) {
	region := strings.TrimSpace(os.Getenv("AWS_REGION"))
	if region == "" {
		region = "us-east-1"
	}
	wagerQueue := strings.TrimSpace(os.Getenv("WAGER_QUEUE_URL"))
	outboxQueue := strings.TrimSpace(os.Getenv("OUTBOX_QUEUE_URL"))
	if wagerQueue == "" || outboxQueue == "" {
		return nil, errors.New("WAGER_QUEUE_URL and OUTBOX_QUEUE_URL are required")
	}
	options := []func(*config.LoadOptions) error{config.WithRegion(region)}
	if accessKey := os.Getenv("AWS_ACCESS_KEY_ID"); accessKey != "" {
		options = append(options, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			accessKey, os.Getenv("AWS_SECRET_ACCESS_KEY"), "")))
	}
	cfg, err := config.LoadDefaultConfig(ctx, options...)
	if err != nil {
		return nil, err
	}
	clientOptions := []func(*sqs.Options){}
	if endpoint := strings.TrimSpace(os.Getenv("AWS_ENDPOINT_URL")); endpoint != "" {
		clientOptions = append(clientOptions, func(o *sqs.Options) { o.BaseEndpoint = aws.String(endpoint) })
	}
	return &Client{
		API:            sqs.NewFromConfig(cfg, clientOptions...),
		WagerQueueURL:  wagerQueue,
		OutboxQueueURL: outboxQueue,
	}, nil
}

func (c *Client) Check(ctx context.Context) error {
	for _, queue := range []string{c.WagerQueueURL, c.OutboxQueueURL} {
		if _, err := c.API.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl:       aws.String(queue),
			AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
		}); err != nil {
			return err
		}
	}
	return nil
}
