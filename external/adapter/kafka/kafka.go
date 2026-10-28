package kafka

import (
	"context"
	"data-producer-service/external/application/send"
	"data-producer-service/external/domain"
	"encoding/json"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	_ send.EventSender = (*RawEventSender)(nil)
)

type RawEventSender struct {
	kafkaClient *kgo.Client
}

func (s *RawEventSender) Send(ctx context.Context, rawPackages []domain.RawPackage) error {
	records := make([]*kgo.Record, 0, len(rawPackages))
	for _, p := range rawPackages {
		record, err := newRecord(p)
		if err != nil {
			return err
		}
		records = append(records, record)
	}

	if err := s.kafkaClient.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return fmt.Errorf("produce %d records: %w", len(records), err)
	}
	return nil
}

func newRecord(p domain.RawPackage) (*kgo.Record, error) {
	value, err := json.Marshal(Map(&p))
	if err != nil {
		return nil, fmt.Errorf("marshal package %s: %w", p.Id, err)
	}
	return &kgo.Record{
		Key:   []byte(p.Id.String()),
		Value: value,
	}, nil
}

func NewRawEventSender(ctx context.Context, brokerAddr []string, topic string) (*RawEventSender, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokerAddr...),
		kgo.DefaultProduceTopic(topic),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}
	err = cl.Ping(ctx)
	if err != nil {
		cl.Close()
		return nil, fmt.Errorf("ping kafka client: %w", err)
	}
	return &RawEventSender{
		kafkaClient: cl,
	}, nil
}

func (s *RawEventSender) Close() {
	s.kafkaClient.Close()
}
