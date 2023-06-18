alias Adbc.Database
alias Adbc.Connection
alias Adbc.Statement
alias Adbc.ArrowArrayStream

Adbc.Driver.download_driver(:sqlite)

{:ok, db} = Adbc.Database.start_link(driver: :sqlite)
{:ok, %Connection{} = conn} = Database.connection(db)

{:ok, statement} = Statement.new(conn)
:ok = Statement.set_sql_query(statement, "CREATE TABLE IF NOT EXISTS foo (i64 INT, f64 REAL, str TEXT)")
{:ok, _stream, _row_affected} = Statement.execute_query(statement)

:ok = Statement.set_sql_query(statement, "INSERT INTO foo(i64,f64,str) VALUES(?,?,?)")
i64_1 = :rand.uniform(1000)
f64_1 = i64_1 * 1.1
str_1 = "value = #{inspect(i64_1)}"
:ok = Statement.bind(statement, [i64_1, f64_1, str_1])
{:ok, _stream, _row_affected} = Statement.execute_query(statement)
:ok = Statement.set_sql_query(statement, "INSERT INTO foo(i64,f64,str) VALUES(?,?,?)")
i64_2 = :rand.uniform(1000)
f64_2 = i64_2 * 1.1
str_2 = "value = #{inspect(i64_2)}"
:ok = Statement.bind(statement, [i64_2, f64_2, str_2])
{:ok, _stream, _row_affected} = Statement.execute_query(statement)

{:ok, statement} = Statement.new(conn)
:ok = Statement.set_sql_query(statement, "SELECT * FROM foo")
{:ok, stream, _row_affected} = Statement.execute_query(statement)
{:ok, next} = ArrowArrayStream.next(stream)
IO.inspect(next)
[
  {"i64", [^i64_1, ^i64_2]},
  {"f64", [^f64_1, ^f64_2]},
  {"str", [^str_1, ^str_2]}
] = next
