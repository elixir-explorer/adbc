defmodule Adbc.Statement.Test do
  use ExUnit.Case
  doctest Adbc.Statement

  alias Adbc.Connection
  alias Adbc.Database
  alias Adbc.Statement

  test "new statement" do
    {:ok, %Database{} = database} = Database.new()
    assert is_reference(database.reference)

    assert :ok == Database.init(database)

    {:ok, %Connection{} = connection} = Connection.new()
    assert is_reference(connection.reference)

    assert :ok == Connection.init(connection, database)
    {:ok, %Statement{} = statement} = Statement.new(connection)
    assert is_reference(statement.reference)

    assert :ok == Connection.release(connection)
    assert :ok == Database.release(database)
  end
end
