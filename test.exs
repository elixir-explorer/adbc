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
:ok = Statement.set_sql_query(statement, "CREATE TABLE IF NOT EXISTS foo (col REAL, str TEXT)")
:ok = Statement.prepare(statement)
{:ok, _stream, _row_affected} = Statement.execute_query(statement)

Statement.set_sql_query(statement, "INSERT INTO foo(col,str) VALUES (?,?)")
Statement.prepare(statement)
:ok = Statement.bind(statement, [:random.uniform(1000), ":random.uniform(1000)"])
{:ok, _stream, _row_affected} = Statement.execute_query(statement)
