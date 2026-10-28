package kafka

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"data-producer-service/external/domain"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

type kafkaTestEnv struct {
	brokers []string
	topic   string
}

func newKafkaTestEnv(t *testing.T) kafkaTestEnv {
	t.Helper()
	brokers, topic := os.Getenv("KAFKA_URL"), os.Getenv("TEST_TOPIC")
	if brokers == "" || topic == "" {
		t.Skip("KAFKA_URL or TEST_TOPIC is not set: skipping Kafka integration test")
	}
	return kafkaTestEnv{brokers: strings.Split(brokers, ","), topic: topic}
}

func newTestSender(t *testing.T, env kafkaTestEnv) *RawEventSender {
	t.Helper()
	sender, err := NewRawEventSender(t.Context(), env.brokers, env.topic)
	if err != nil {
		t.Fatalf("NewRawEventSender() error = %v", err)
	}
	t.Cleanup(sender.Close)
	return sender
}

func newTestConsumer(t *testing.T, env kafkaTestEnv) *kgo.Client {
	t.Helper()
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(env.brokers...),
		kgo.ConsumeTopics(env.topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	t.Cleanup(consumer.Close)
	return consumer
}

func TestRawEventSenderSendsKeyAndJSON(t *testing.T) {
	env := newKafkaTestEnv(t)
	sender := newTestSender(t, env)
	consumer := newTestConsumer(t, env)

	packages := []domain.RawPackage{
		{Id: uuid.New(), CreatedAt: time.Now(), RandomNumber: []int{1, 2}},
		{Id: uuid.New(), CreatedAt: time.Now(), RandomNumber: []int{3}},
	}
	want := make(map[string]string, len(packages))
	for _, p := range packages {
		value, err := json.Marshal(Map(&p))
		if err != nil {
			t.Fatalf("json.Marshal() error = %v", err)
		}
		want[p.Id.String()] = string(value)
	}

	if err := sender.Send(t.Context(), packages); err != nil {
		t.Fatalf("Send() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	for len(want) > 0 {
		fetches := consumer.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			t.Fatalf("PollFetches() error = %v; still not found: %v", err, want)
		}
		for _, r := range fetches.Records() {
			key := string(r.Key)
			wantValue, ours := want[key]
			if !ours {
				continue
			}
			if string(r.Value) != wantValue {
				t.Errorf("value for key %s =\n%s\nwant\n%s", key, r.Value, wantValue)
			}
			delete(want, key)
		}
	}
}
