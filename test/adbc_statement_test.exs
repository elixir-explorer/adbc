defmodule Adbc.Statement.Test do
  use ExUnit.Case
  doctest Adbc.Statement

  alias Adbc.Connection
  alias Adbc.Database
  alias Adbc.Statement

  setup do
    db = start_supervised!({Database, driver: :sqlite})
    {:ok, conn} = Database.connection(db)
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
end
