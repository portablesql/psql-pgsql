[![Go Reference](https://pkg.go.dev/badge/github.com/portablesql/psql-pgsql.svg)](https://pkg.go.dev/github.com/portablesql/psql-pgsql)

# psql-pgsql

PostgreSQL / CockroachDB driver for [portablesql/psql](https://github.com/portablesql/psql).

## Installation

```bash
go get github.com/portablesql/psql-pgsql
```

## Usage

Import with a blank identifier to register the driver automatically:

```go
import (
    "github.com/portablesql/psql"
    _ "github.com/portablesql/psql-pgsql"
)

be, err := psql.New("postgresql://user:password@localhost:5432/mydb")
ctx := be.Plug(context.Background())
```

DSNs starting with `postgresql://` or `postgres://` are matched automatically.

## Features

- `$1, $2, ...` numbered placeholders
- `RETURNING` clause support for INSERT/UPDATE
- `ON CONFLICT ... DO UPDATE SET` and `DO NOTHING` for upserts
- `= ANY($1)` array comparison (via `psql.Any{}`)
- `ILIKE` for case-insensitive pattern matching
- Vector similarity search with `<->` (L2), `<=>` (cosine), and `<#>` (inner product) operators
- `JSONB` for JSON columns
- `TIMESTAMP(6)` with microsecond precision
- Automatic table creation and schema migration via `information_schema`
- Connection pooling via [pgxpool](https://pkg.go.dev/github.com/jackc/pgx/v5/pgxpool)
- Duplicate detection via SQLSTATE `23505`

## Underlying Driver

[github.com/jackc/pgx/v5](https://github.com/jackc/pgx) with stdlib adapter for `database/sql` compatibility.

## License

MIT - see [LICENSE](LICENSE).
