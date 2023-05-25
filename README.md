# Adbc

### SQLite Example

```elixir
alias Adbc.Database
alias Adbc.Connection
alias Adbc.Statement

# new and init an SQLite database with filename as my_db.db
{:ok, database} = Database.new()
Database.set_option(database, "driver", "adbc_driver_sqlite")
Database.set_option(database, "uri", "file:my_db.db")
Database.init(database)

# init the connection
{:ok, connection} = Connection.new()
:ok = Connection.init(connection, database)

# init statement with the connection
{:ok, statement} = Statement.new(connection)

# execute query
# this creates a table if not exists
Statement.set_sql_query(statement, "CREATE TABLE foo (col) IF NOT EXISTS")
Statement.prepare(statement)
{:ok, _stream, _row_affected} = Statement.execute_query(statement)

# execute another query
# this inserts a row with value 42 into the foo table
Statement.set_sql_query(statement, "INSERT INTO foo VALUES (42)")
Statement.prepare(statement)
{:ok, _stream, _row_affected} = Statement.execute_query(statement)

# release resources
Statement.release(statement)
Connection.release(connection)
Database.release(database)
```

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `adbc` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:adbc, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/adbc>.

