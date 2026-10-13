package domain

import (
	"time"

	"github.com/google/uuid"
)

type IntegrationEvent struct {
	EventID       string               `json:"eventId"`
	EventType     string               `json:"eventType"`
	AggregateID   string               `json:"aggregateId"`
	CorrelationID string               `json:"correlationId"`
	CausationID   string               `json:"causationId,omitempty"`
	OccurredAt    time.Time            `json:"occurredAt"`
	Version       int                  `json:"version"`
	Data          IntegrationEventData `json:"data"`
}

type IntegrationEventData interface {
	integrationEventData()
}

type WagerTransactionEventData struct {
	TransactionID string            `json:"transactionId"`
	ProviderID    string            `json:"providerId,omitempty"`
	ExternalID    string            `json:"externalTransactionId,omitempty"`
	WalletID      string            `json:"walletId"`
	Status        TransactionStatus `json:"status"`
	FailureCode   string            `json:"failureCode,omitempty"`
	Balance       Money             `json:"balance"`
}

type WalletBalanceChangedData struct {
	WalletID      string `json:"walletId"`
	TransactionID string `json:"transactionId"`
	Direction     string `json:"direction"`
	Money         Money  `json:"money"`
	BalanceBefore Money  `json:"balanceBefore"`
	BalanceAfter  Money  `json:"balanceAfter"`
	WalletVersion int    `json:"walletVersion"`
}

func (WagerTransactionEventData) integrationEventData() {}
func (WalletBalanceChangedData) integrationEventData()  {}

func newIntegrationEvent(eventType, aggregateID, correlationID, causationID string, data IntegrationEventData) IntegrationEvent {
	return IntegrationEvent{
		EventID: uuid.NewString(), EventType: eventType, AggregateID: aggregateID,
		CorrelationID: correlationID, CausationID: causationID,
		OccurredAt: time.Now().UTC(), Version: 1, Data: data,
	}
}

func NewWagerTransactionProcessedEvent(aggregateID, correlationID, causationID string, data WagerTransactionEventData) IntegrationEvent {
	return newIntegrationEvent("WagerTransactionProcessed", aggregateID, correlationID, causationID, data)
}

func NewWagerTransactionRejectedEvent(aggregateID, correlationID, causationID string, data WagerTransactionEventData) IntegrationEvent {
	return newIntegrationEvent("WagerTransactionRejected", aggregateID, correlationID, causationID, data)
}

func NewWagerTransactionPendingReferenceEvent(aggregateID, correlationID, causationID string, data WagerTransactionEventData) IntegrationEvent {
	return newIntegrationEvent("WagerTransactionPendingReference", aggregateID, correlationID, causationID, data)
}

func NewWalletBalanceChangedEvent(aggregateID, correlationID, causationID string, data WalletBalanceChangedData) IntegrationEvent {
	return newIntegrationEvent("WalletBalanceChanged", aggregateID, correlationID, causationID, data)
}
