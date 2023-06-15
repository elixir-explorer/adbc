# Adbc

### SQLite Example

```elixir
iex> alias Adbc.Database
Adbc.Database
iex> alias Adbc.Connection
Adbc.Connection
iex> alias Adbc.Statement
Adbc.Statement

# new and init an SQLite database with filename as my_db.db
iex> {:ok, database} = Database.new()
{:ok, %Adbc.Database{reference: #Reference<0.1918355494.3778674689.52121>}}
iex> Database.set_option(database, "driver", "adbc_driver_sqlite")
:ok
iex> Database.set_option(database, "uri", "file:my_db1.db")
:ok
iex> Database.it(database)
:ok

# init the connection
iex> {:ok, connection} = Connection.new()
{:ok, %Adbc.Connection{reference: #Reference<0.1918355494.3778674689.52179>}}
iex> :ok = Connection.init(connection, database)
:ok

# init statement with the connection
iex> {:ok, statement} = Statement.new(connection)
{:ok, %Adbc.Statement{reference: #Reference<0.1918355494.3778674689.52210>}}

# execute query
# this creates a table if not exists
iex> Statement.set_sql_query(statement, "CREATE TABLE IF NOT EXISTS foo (col REAL, str TEXT)")
:ok
iex> Statement.prepare(statement)
:ok
iex> {:ok, _stream, _row_affected} = Statement.execute_query(statement)
{:ok,
 %Adbc.ArrowArrayStream{reference: #Reference<0.1918355494.3778674689.52253>},
 -1}

# execute another query
# this inserts a row with value 42 into the foo table
iex> Statement.set_sql_query(statement, "INSERT INTO foo (col,str) VALUES (?,?)")
:ok
iex> Statement.prepare(statement)
:ok
iex> r = :random.uniform(1000)
iex> :ok = Statement.bind(statement, [r, "value = #{r + 1}"])
iex> {:ok, _stream, _row_affected} = Statement.execute_query(statement)
{:ok,
 %Adbc.ArrowArrayStream{reference: #Reference<0.1918355494.3778674689.52300>},
 -1}

# release resources
iex> Statement.release(statement)
:ok
iex> Connection.release(connection)
:ok
iex> Database.release(database)
:ok
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
