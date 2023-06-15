defmodule Adbc.Connection.Test do
  use ExUnit.Case
  doctest Adbc.Connection

  alias Adbc.ArrowArrayStream
  alias Adbc.Connection
  alias Adbc.Database

  setup do
    db = start_supervised!({Adbc.Database, driver: :sqlite})
    %{db: db}
  end

  describe "adbc connection metadata" do
    test "get all info from a connection", %{db: db} do
      {:ok, %Connection{} = conn} = Database.connection(db)
      assert is_reference(conn.reference)

      {:ok, %ArrowArrayStream{} = array_stream} = Connection.get_info(conn)
      assert is_reference(array_stream.reference)
    end

    test "get all objects from a connection", %{db: db} do
      {:ok, %Connection{} = conn} = Database.connection(db)
      assert is_reference(conn.reference)

      {:ok, %ArrowArrayStream{} = array_stream} = Connection.get_objects(conn, 0)
      assert is_reference(array_stream.reference)
    end

    # test "get table schema from a connection" do
    #   {:ok, %Connection{} = conn} = Database.connection(db)
    #   assert is_reference(conn.reference)
    #
    #   {:ok, %ArrowSchema{} = schema} = Connection.get_table_schema(conn, nil, nil, "table")
    #   assert is_reference(schema.reference)
    # end

    test "get table types from a connection", %{db: db} do
      {:ok, %Connection{} = conn} = Database.connection(db)
      assert is_reference(conn.reference)

      {:ok, %ArrowArrayStream{} = array_stream} = Connection.get_table_types(conn)
      assert is_reference(array_stream.reference)
    end
  end

  describe "adbc connection partition" do
    # test "read partition" do
    #   {:ok, %Connection{} = conn} = Database.connection(db)
    #   assert is_reference(conn.reference)
    #
    #   partition_data = << >>
    #   {:ok, %ArrowArrayStream{} = array_stream} = Connection.read_partition(conn, partition_data)
    #   assert is_reference(array_stream.reference)
    # end
  end
end
