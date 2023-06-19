defmodule Adbc.Connection.Test do
  use ExUnit.Case
  doctest Adbc.Connection

  alias Adbc.{ArrowArrayStream, ArrowSchema}
  alias Adbc.Connection

  setup do
    db = start_supervised!({Adbc.Database, driver: :sqlite})
    %{db: db}
  end

  describe "start_link" do
    test "starts a process", %{db: db} do
      assert {:ok, pid} = Connection.start_link(database: db)
      assert is_pid(pid)
    end

    test "accepts process options", %{db: db} do
      assert {:ok, pid} =
               Connection.start_link(database: db, process_options: [name: :who_knows_conn])

      assert Process.whereis(:who_knows_conn) == pid
    end

    @tag :capture_log
    test "terminates when database terminates", %{db: db} do
      Process.flag(:trap_exit, true)
      assert {:ok, pid} = Connection.start_link(database: db)
      ref = Process.monitor(pid)
      Process.exit(db, :kill)
      assert_receive {:DOWN, ^ref, _, _, _}
    end
  end

  describe "get_info" do
    test "get all info from a connection", %{db: db} do
      conn = start_supervised!({Connection, database: db})
      {:ok, %ArrowArrayStream{} = array_stream} = Connection.get_info(conn)
      assert is_reference(array_stream.reference)
    end

    test "get some info from a connection", %{db: db} do
      conn = start_supervised!({Connection, database: db})
      {:ok, %ArrowArrayStream{} = array_stream} = Connection.get_info(conn, [1])
      assert is_reference(array_stream.reference)
    end
  end

  describe "get_objects" do
    test "get all objects from a connection", %{db: db} do
      conn = start_supervised!({Connection, database: db})
      {:ok, %ArrowArrayStream{} = array_stream} = Connection.get_objects(conn, 0)
      assert is_reference(array_stream.reference)
    end
  end

  describe "get_table_types" do
    test "get table types from a connection", %{db: db} do
      conn = start_supervised!({Connection, database: db})
      {:ok, %ArrowArrayStream{} = array_stream} = Connection.get_table_types(conn)
      assert is_reference(array_stream.reference)
    end
  end

  describe "get_table_schema" do
    @tag :skip
    test "get table schema from a connection", %{db: db} do
      conn = start_supervised!({Connection, database: db})
      {:ok, %ArrowSchema{} = schema} = Connection.get_table_schema(conn, nil, nil, "table")
      assert is_reference(schema.reference)
    end

    test "returns error when table does not exist", %{db: db} do
      conn = start_supervised!({Connection, database: db})
      {:error, %Adbc.Error{}} = Connection.get_table_schema(conn, nil, nil, "table")
    end
  end
end
