defmodule Adbc.Statement.Test do
  use ExUnit.Case
  doctest Adbc.Statement

  alias Adbc.Connection
  alias Adbc.Database
  alias Adbc.Statement

  test "new statement and release it" do
    {:ok, %Database{} = database} = Database.new()
    assert is_reference(database.reference)
    :ok = Database.set_option(database, "driver", "adbc_driver_sqlite")

    assert :ok == Database.init(database)

    {:ok, %Connection{} = connection} = Connection.new()
    assert is_reference(connection.reference)

    assert :ok == Connection.init(connection, database)
    {:ok, %Statement{} = statement} = Statement.new(connection)
    assert is_reference(statement.reference)

    assert :ok == Statement.release(statement)

    assert :ok == Connection.release(connection)
    assert :ok == Database.release(database)
  end

  test "release a statement twice should raise an ArgumentError" do
    {:ok, %Database{} = database} = Database.new()
    assert is_reference(database.reference)
    :ok = Database.set_option(database, "driver", "adbc_driver_sqlite")

    assert :ok == Database.init(database)

    {:ok, %Connection{} = connection} = Connection.new()
    assert is_reference(connection.reference)

    assert :ok == Connection.init(connection, database)
    {:ok, %Statement{} = statement} = Statement.new(connection)
    assert is_reference(statement.reference)

    assert :ok == Statement.release(statement)
    assert {:error, "invalid state"} == Statement.release(statement)

    assert :ok == Connection.release(connection)
    assert :ok == Database.release(database)
  end

  test "execute statements" do
    {:ok, %Database{} = database} = Database.new()
    assert is_reference(database.reference)
    :ok = Database.set_option(database, "driver", "adbc_driver_sqlite")

    assert :ok == Database.init(database)

    {:ok, %Connection{} = connection} = Connection.new()
    assert is_reference(connection.reference)

    assert :ok == Connection.init(connection, database)
    {:ok, %Statement{} = statement} = Statement.new(connection)
    assert is_reference(statement.reference)
    assert :ok == Statement.set_sql_query(statement, "CREATE TABLE foo (col)")
    assert :ok == Statement.prepare(statement)

    {:ok, _stream, row_affected} = Statement.execute_query(statement)
    assert row_affected == -1

    assert :ok == Statement.set_sql_query(statement, "INSERT INTO foo VALUES (42)")
    assert :ok == Statement.prepare(statement)

    {:ok, _stream, row_affected} = Statement.execute_query(statement)
    assert row_affected == -1

    assert :ok == Statement.release(statement)

    assert :ok == Connection.release(connection)
    assert :ok == Database.release(database)
  end
end
