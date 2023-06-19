defmodule Adbc.Statement.Test do
  use ExUnit.Case
  doctest Adbc.Statement

  alias Adbc.Statement
  alias Adbc.ArrowArrayStream

  setup do
    db = start_supervised!({Adbc.Database, driver: :sqlite})
    conn = %{reference: :sys.get_state(start_supervised!({Adbc.Connection, database: db}))}
    %{db: db, conn: conn}
  end

  test "new statement and release it", %{conn: conn} do
    {:ok, %Statement{} = statement} = Statement.new(conn)
    assert is_reference(statement.reference)
    assert :ok == Statement.release(statement)
  end

  test "release a statement twice should raise an ArgumentError", %{conn: conn} do
    {:ok, %Statement{} = statement} = Statement.new(conn)
    assert is_reference(statement.reference)
    assert :ok == Statement.release(statement)
    assert {:error, "invalid state"} == Statement.release(statement)
  end

  test "execute statements", %{conn: conn, test: test} do
    {:ok, %Statement{} = statement} = Statement.new(conn)
    assert is_reference(statement.reference)
    assert :ok == Statement.set_sql_query(statement, "CREATE TABLE \"#{test}\" (col)")
    {:ok, _stream, row_affected} = Statement.execute_query(statement)
    assert row_affected == -1

    assert :ok == Statement.set_sql_query(statement, "INSERT INTO \"#{test}\" VALUES (42)")
    {:ok, _stream, row_affected} = Statement.execute_query(statement)
    assert row_affected == -1

    assert :ok == Statement.set_sql_query(statement, "INSERT INTO \"#{test}\" VALUES (43)")
    assert :ok == Statement.prepare(statement)
    {:ok, _stream, row_affected} = Statement.execute_query(statement)
    assert row_affected == -1
  end

  test "bind statement", %{conn: conn, test: test} do
    {:ok, %Statement{} = statement} = Statement.new(conn)
    assert is_reference(statement.reference)
    assert :ok == Statement.set_sql_query(statement, "CREATE TABLE \"#{test}\" (col)")
    {:ok, _stream, row_affected} = Statement.execute_query(statement)
    assert row_affected == -1

    assert :ok == Statement.set_sql_query(statement, "INSERT INTO \"#{test}\" VALUES (?)")
    assert :ok == Statement.bind(statement, [42])
    {:ok, _stream, row_affected} = Statement.execute_query(statement)
    assert row_affected == -1

    assert :ok == Statement.set_sql_query(statement, "INSERT INTO \"#{test}\" VALUES (?)")
    assert :ok == Statement.bind(statement, [43])
    assert :ok == Statement.prepare(statement)
    {:ok, _stream, row_affected} = Statement.execute_query(statement)
    assert row_affected == -1
  end

  test "query values", %{conn: conn, test: test} do
    {:ok, %Statement{} = statement} = Statement.new(conn)
    assert is_reference(statement.reference)
    assert :ok == Statement.set_sql_query(statement, "CREATE TABLE \"#{test}\" (i64 INT, f64 REAL, str TEXT)")
    {:ok, _stream, row_affected} = Statement.execute_query(statement)
    assert row_affected == -1

    assert :ok == Statement.set_sql_query(statement, "INSERT INTO \"#{test}\" VALUES (?,?,?)")
    assert :ok == Statement.bind(statement, [42, 42.0, "value = 42"])
    {:ok, _stream, row_affected} = Statement.execute_query(statement)
    assert row_affected == -1

    assert :ok == Statement.set_sql_query(statement, "INSERT INTO \"#{test}\" VALUES (?,?,?)")
    assert :ok == Statement.bind(statement, [43, 43.0, "value = 43"])
    # assert :ok == Statement.prepare(statement)
    {:ok, _stream, row_affected} = Statement.execute_query(statement)
    assert row_affected == -1

    assert :ok == Statement.set_sql_query(statement, "INSERT INTO \"#{test}\" VALUES (?,?,?)")
    assert :ok == Statement.bind(statement, [44, 44.0, "value = 44"])
    # assert :ok == Statement.prepare(statement)
    {:ok, _stream, row_affected} = Statement.execute_query(statement)
    assert row_affected == -1

    {:ok, %Statement{} = statement} = Statement.new(conn)
    assert :ok == Statement.set_sql_query(statement, "SELECT * FROM \"#{test}\" WHERE i64 > 42")
    {:ok, stream, row_affected} = Statement.execute_query(statement)
    assert row_affected == -1
    {:ok, next} = ArrowArrayStream.next(stream)
    assert [
      {"i64", [43, 44]},
      {"f64", [43.0, 44.0]},
      {"str", ["value = 43", "value = 44"]}
    ] == next

    assert :ok == Statement.set_sql_query(statement, "SELECT * FROM \"#{test}\" WHERE i64 = ?")
    assert :ok == Statement.bind(statement, [42])
    {:ok, stream, row_affected} = Statement.execute_query(statement)
    assert row_affected == -1
    {:ok, next} = ArrowArrayStream.next(stream)
    assert [
      {"i64", [42]},
      {"f64", [42.0]},
      {"str", ["value = 42"]}
    ] == next

    Statement.release(statement)
  end
end
