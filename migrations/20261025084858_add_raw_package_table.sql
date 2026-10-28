-- +goose Up
create table packages (
                          id            uuid        primary key,
                          created_at    timestamptz not null,
                          sent_at       timestamptz,
                          random_number bigint[]    not null
);
create index idx_packages_unsent on packages (created_at) where sent_at is null;

-- +goose Down
drop table packages;