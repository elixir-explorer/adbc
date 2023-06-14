alias Adbc.Database
alias Adbc.Connection
alias Adbc.Statement

{:ok, database} = Database.new()
Database.set_option(database, "driver", "adbc_driver_sqlite")
Database.set_option(database, "uri", "file:my_db1.db")
Database.init(database)

{:ok, connection} = Connection.new()
:ok = Connection.init(connection, database)

{:ok, statement} = Statement.new(connection)
:ok = Statement.set_sql_query(statement, "CREATE TABLE IF NOT EXISTS foo (i64 INT, f64 REAL, str TEXT)")
:ok = Statement.prepare(statement)
{:ok, _stream, _row_affected} = Statement.execute_query(statement)

:ok = Statement.set_sql_query(statement, "INSERT INTO foo(i64,f64,str) VALUES(?,?,?)")
:ok = Statement.prepare(statement)
r = :random.uniform(1000)
:ok = Statement.bind(statement, [r, r * 1.1, "value = #{inspect(r)}"])
{:ok, _stream, _row_affected} = Statement.execute_query(statement)

:ok = Statement.set_sql_query(statement, "INSERT INTO foo(str,i64,f64) VALUES(?,?,?)")
:ok = Statement.prepare(statement)
r = :random.uniform(1000)
:ok = Statement.bind(statement, ["value = #{inspect(r)}", r, r * 1.1])
{:ok, _stream, _row_affected} = Statement.execute_query(statement)
