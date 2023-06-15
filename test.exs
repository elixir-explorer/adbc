alias Adbc.Database
alias Adbc.Connection
alias Adbc.Statement
alias Adbc.ArrowArrayStream

{:ok, database} = Database.new()
Database.set_option(database, "driver", "adbc_driver_sqlite")
Database.init(database)

{:ok, connection} = Connection.new()
:ok = Connection.init(connection, database)

{:ok, statement} = Statement.new(connection)
:ok = Statement.set_sql_query(statement, "CREATE TABLE IF NOT EXISTS foo (i64 INT, f64 REAL, str TEXT)")
{:ok, _stream, _row_affected} = Statement.execute_query(statement)

:ok = Statement.set_sql_query(statement, "INSERT INTO foo(i64,f64,str) VALUES(?,?,?)")
r = :rand.uniform(1000)
:ok = Statement.bind(statement, [r, r * 1.1, "value = #{inspect(r)}"])
{:ok, _stream, _row_affected} = Statement.execute_query(statement)
:ok = Statement.set_sql_query(statement, "INSERT INTO foo(i64,f64,str) VALUES(?,?,?)")
r = :rand.uniform(1000)
:ok = Statement.bind(statement, [r, r * 1.1, "value = #{inspect(r)}"])
{:ok, _stream, _row_affected} = Statement.execute_query(statement)

{:ok, statement} = Statement.new(connection)
:ok = Statement.set_sql_query(statement, "SELECT * FROM foo")
{:ok, stream, _row_affected} = Statement.execute_query(statement)
{:ok, schema} = ArrowArrayStream.get_schema(stream)
IO.inspect(schema)
