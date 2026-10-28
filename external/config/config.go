package config

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

type Database struct {
	Dsn string `yaml:"dsn" env:"DSN_URL"`
}

type Kafka struct {
	Url   []string `yaml:"url" env:"KAFKA_URL" env-required:"true"`
	Topic string   `yaml:"topic" env:"KAFKA_TOPIC" env-required:"true"`
}

type Scheduler struct {
	ScheduleInterval  time.Duration `yaml:"schedule-interval" env:"SCHEDULE_INTERVAL" env-required:"true"`
	SendInterval      time.Duration `yaml:"send-interval" env:"SEND_INTERVAL" env-required:"true"`
	IterationTimeout  time.Duration `yaml:"iteration-timeout" env:"ITERATION_TIMEOUT" env-required:"true"`
	RandomNumberCount int           `yaml:"random-number-count" env:"RANDOM_NUMBER_COUNT" env-required:"true"`
	SendLimit         int           `yaml:"send-limit" env:"SEND_LIMIT" env-required:"true"`
}

func (s Scheduler) validate() error {
	var errs []error
	for name, d := range map[string]time.Duration{
		"schedule-interval": s.ScheduleInterval,
		"send-interval":     s.SendInterval,
		"iteration-timeout": s.IterationTimeout,
	} {
		if d <= 0 {
			errs = append(errs, fmt.Errorf("scheduler.%s must be positive, got %s", name, d))
		}
	}
	return errors.Join(errs...)
}

type Config struct {
	DB        Database  `yaml:"database"`
	Kafka     Kafka     `yaml:"kafka"`
	Scheduler Scheduler `yaml:"scheduler"`
}

func LoadConfig() (*Config, error) {
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		return nil, fmt.Errorf("CONFIG_PATH environment variable not set")
	}
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("CONFIG_PATH file does not exist")
	}
	var cfg Config
	if err := cleanenv.ReadConfig(configPath, &cfg); err != nil {
		return nil, fmt.Errorf("unable to read config: %w", err)
	}
	if err := cfg.Scheduler.validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return &cfg, nil
}
