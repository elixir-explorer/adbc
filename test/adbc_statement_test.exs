defmodule Adbc.Statement.Test do
  use ExUnit.Case
  doctest Adbc.Statement

  alias Adbc.Connection
  alias Adbc.Database
  alias Adbc.Statement

  setup do
    {:ok, %Database{} = database} = Database.new()
    :ok = Database.set_option(database, "driver", "adbc_driver_sqlite")
    :ok = Database.init(database)

    {:ok, %Connection{} = connection} = Connection.new()
    :ok = Connection.init(connection, database)

    on_exit(fn ->
      Connection.release(connection)
      Database.release(database)
    end)

    %{database: database, connection: connection}
  end

  test "new statement and release it", %{connection: connection} do
    {:ok, %Statement{} = statement} = Statement.new(connection)
    assert is_reference(statement.reference)
    assert :ok == Statement.release(statement)
  end

  test "release a statement twice should raise an ArgumentError", %{connection: connection} do
    {:ok, %Statement{} = statement} = Statement.new(connection)
    assert is_reference(statement.reference)
    assert :ok == Statement.release(statement)
    assert {:error, "invalid state"} == Statement.release(statement)
  end

  test "execute statements", %{connection: connection, test: test} do
    {:ok, %Statement{} = statement} = Statement.new(connection)
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

  test "bind statement", %{connection: connection, test: test} do
    {:ok, %Statement{} = statement} = Statement.new(connection)
    assert is_reference(statement.reference)
    assert :ok == Statement.set_sql_query(statement, "CREATE TABLE \"#{test}\" (col)")
    {:ok, _stream, row_affected} = Statement.execute_query(statement)
    assert row_affected == -1

    assert :ok == Statement.set_sql_query(statement, "INSERT INTO \"#{test}\" VALUES (?)")
    assert :ok == Statement.bind(statement, [42])
    {:ok, _stream, row_affected} = Statement.execute_query(statement)
    assert row_affected == -1

    assert :ok == Statement.set_sql_query(statement, "INSERT INTO \"#{test}\" VALUES (?)")
    assert :ok == Statement.bind(statement, [3])
    assert :ok == Statement.prepare(statement)
    {:ok, _stream, row_affected} = Statement.execute_query(statement)
    assert row_affected == -1
  end
end
