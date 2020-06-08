package pgxhook

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"time"
)

const defaultTimeout = time.Millisecond * 100

type PGXHook struct {
	db        *pgxpool.Pool
	timeout   time.Duration
	tableName string
}

// NewHook - create new pgx logrus hook
func NewHook(db *pgxpool.Pool, tableName string, timeout time.Duration) (hook *PGXHook, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	if _, err = db.Exec(ctx, "SELECT version();"); err != nil {
		return
	}
	hook = &PGXHook{
		db:        db,
		timeout:   timeout,
		tableName: tableName,
	}

	// default timeout
	if hook.timeout == 0 {
		hook.timeout = defaultTimeout
	}
	if err = hook.createTable(); err != nil {
		return
	}
	return
}

func (hook *PGXHook) Fire(entry *logrus.Entry) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), hook.timeout)
	defer cancel()

	str, err := entry.String()
	if err != nil {
		err = errors.Wrap(err, "unable to read logrus entry")
		return
	}

	query := `
INSERT INTO logs
(
	timestamp,
	level,
	message,
	full_message
)
VALUES ($1, $2, $3, $4)
`
	if _, err = hook.db.Exec(ctx, query,
		entry.Time,
		entry.Level.String(),
		entry.Message,
		str,
	); err != nil {
		err = errors.Wrap(err, "unable to insert log entry")
		return
	}

	return
}

func (hook *PGXHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (hook *PGXHook) createTable() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), hook.timeout)
	defer cancel()

	query := fmt.Sprintf(`create table if not exists %s
(
	id serial,
	timestamp timestamp with time zone not null,
	level varchar(10) not null,
	message TEXT not null,
	full_message TEXT not null
);
create index if not exists logs_level_index
	on logs (level);
create index if not exists logs_timestamp_index
	on logs (timestamp);
`, hook.tableName)
	if _, err = hook.db.Exec(ctx, query); err != nil {
		err = errors.Wrapf(err, "unable to initialize logrus hook table named %s", hook.tableName)
		return
	}

	return
}
