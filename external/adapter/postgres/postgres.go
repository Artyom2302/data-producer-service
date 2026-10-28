package postgres

import (
	"context"
	"data-producer-service/external/application/schedule"
	"data-producer-service/external/application/send"
	"data-producer-service/external/domain"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	_ schedule.EventSaver = (*PgStorage)(nil)
	_ send.EventStorage   = (*PgStorage)(nil)
)

type PgStorage struct {
	pool *pgxpool.Pool
}

func NewPgStorage(ctx context.Context, dsn string) (*PgStorage, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &PgStorage{pool: pool}, nil
}

func (p *PgStorage) Save(ctx context.Context, rawPackage domain.RawPackage) error {
	const query = `insert into packages (id, created_at, random_number) values ($1, $2, $3)`
	if _, err := p.pool.Exec(ctx, query, rawPackage.Id, rawPackage.CreatedAt, rawPackage.RandomNumber); err != nil {
		return fmt.Errorf("insert package %s: %w", rawPackage.Id, err)
	}
	return nil
}

func (p *PgStorage) Receive(ctx context.Context, limitCount int) ([]domain.RawPackage, error) {
	const query = `
		select id, created_at, random_number
		from packages
		where sent_at is null
		order by created_at
		limit $1`
	rows, err := p.pool.Query(ctx, query, limitCount)
	if err != nil {
		return nil, fmt.Errorf("select unsent packages: %w", err)
	}
	packages, err := pgx.CollectRows(rows, pgx.RowToStructByPos[domain.RawPackage])
	if err != nil {
		return nil, fmt.Errorf("collect unsent packages: %w", err)
	}
	return packages, nil
}

func (p *PgStorage) MarkSent(ctx context.Context, ids []uuid.UUID) error {
	const query = `
		update packages
		set sent_at = now()
		where id = any($1) and sent_at is null`
	if _, err := p.pool.Exec(ctx, query, ids); err != nil {
		return fmt.Errorf("mark %d packages as sent: %w", len(ids), err)
	}
	return nil
}

func (p *PgStorage) Close() {
	p.pool.Close()
}
