defmodule Adbc.Database.Test do
  use ExUnit.Case
  doctest Adbc.Database
  alias Adbc.Database

  test "allocate a database, init it and then release it" do
    {:ok, %Database{} = database} = Database.new()
    assert is_reference(database.reference)

    assert :ok == Database.init(database)
    assert :ok == Database.release(database)
  end

  test "release a database twice should raise an ArgumentError" do
    {:ok, %Database{} = database} = Database.new()
    assert is_reference(database.reference)

    assert :ok == Database.init(database)
    assert :ok == Database.release(database)

    assert_raise ArgumentError, fn ->
      Database.release(database)
    end
  end
end
