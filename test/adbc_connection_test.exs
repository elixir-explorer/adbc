defmodule Adbc.Connection.Test do
  use ExUnit.Case
  doctest Adbc.Connection

  alias Adbc.ArrowArrayStream
  alias Adbc.Connection
  alias Adbc.Database

  test "allocate a connection, init it and then release it" do
    {:ok, %Database{} = database} = Database.new()
    assert is_reference(database.reference)

    assert :ok == Database.init(database)

    {:ok, %Connection{} = connection} = Connection.new()
    assert is_reference(connection.reference)

    assert :ok == Connection.init(connection, database)
    assert :ok == Connection.release(connection)

    assert :ok == Database.release(database)
  end

  test "release a connection twice should raise an ArgumentError" do
    {:ok, %Database{} = database} = Database.new()
    assert is_reference(database.reference)

    assert :ok == Database.init(database)

    {:ok, %Connection{} = connection} = Connection.new()
    assert is_reference(connection.reference)

    assert :ok == Connection.init(connection, database)
    assert :ok == Connection.release(connection)

    assert :ok == Database.release(database)

    assert_raise ArgumentError, fn ->
      Connection.release(connection)
    end
  end

  test "get all info from a connection" do
    {:ok, %Database{} = database} = Database.new()
    assert is_reference(database.reference)

    assert :ok == Database.init(database)

    {:ok, %Connection{} = connection} = Connection.new()
    assert is_reference(connection.reference)

    assert :ok == Connection.init(connection, database)
    {:ok, %ArrowArrayStream{} = array_stream} = Connection.get_info(connection)
    assert is_reference(array_stream.reference)
    assert :ok == Connection.release(connection)

    assert :ok == Database.release(database)
  end

  test "get all objects from a connection" do
    {:ok, %Database{} = database} = Database.new()
    assert is_reference(database.reference)

    assert :ok == Database.init(database)

    {:ok, %Connection{} = connection} = Connection.new()
    assert is_reference(connection.reference)

    assert :ok == Connection.init(connection, database)
    {:ok, %ArrowArrayStream{} = array_stream} = Connection.get_objects(connection, 0)
    assert is_reference(array_stream.reference)
    assert :ok == Connection.release(connection)

    assert :ok == Database.release(database)
  end
end
