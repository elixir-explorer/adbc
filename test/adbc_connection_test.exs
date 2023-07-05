defmodule Adbc.Connection.Test do
  use ExUnit.Case
  doctest Adbc.Connection

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

    test "errors with invalid option", %{db: db} do
      Process.flag(:trap_exit, true)

      assert {:error, %Adbc.Error{} = error} = Connection.start_link(database: db, who_knows: 123)

      assert Exception.message(error) == "[SQLite] Unknown connection option who_knows=123"
    end
  end

  describe "get_info" do
    @tag skip: "fails with bad tag"
    test "get all info from a connection", %{db: db} do
      conn = start_supervised!({Connection, database: db})
      {:ok, :done} = Connection.get_info(conn)
    end

    @tag skip: "fails with bad tag"
    test "get some info from a connection", %{db: db} do
      conn = start_supervised!({Connection, database: db})
      {:ok, :done} = Connection.get_info(conn, [1])
    end
  end

  describe "get_objects" do
    @tag skip: "fails with bad tag"
    test "get all objects from a connection", %{db: db} do
      conn = start_supervised!({Connection, database: db})
      {:ok, :done} = Connection.get_objects(conn, 0)
    end
  end

  describe "get_table_types" do
    test "get table types from a connection", %{db: db} do
      conn = start_supervised!({Connection, database: db})
      assert {:ok, %{"table_type" => ["table", "view"]}} = Connection.get_table_types(conn)
    end
  end

  describe "query" do
    test "select", %{db: db} do
      conn = start_supervised!({Connection, database: db})

      assert {:ok, %{"num" => [123]}} = Connection.query(conn, "SELECT 123 as num")

      assert {:ok, %{"num" => [123], "bool" => [1]}} =
               Connection.query(conn, "SELECT 123 as num, true as bool")
    end

    test "fails on invalid query", %{db: db} do
      conn = start_supervised!({Connection, database: db})
      assert {:error, %Adbc.Error{} = error} = Connection.query(conn, "NOT VALID SQL")
      assert Exception.message(error) =~ "[SQLite] Failed to prepare query"
    end
  end

  describe "lock" do
    test "serializes access", %{db: db} do
      conn = start_supervised!({Connection, database: db})

      for _ <- 1..10 do
        Task.async(fn -> run_anything(conn) end)
      end
      |> Task.await_many()
    end

    test "crashes releases the lock", %{db: db} do
      conn = start_supervised!({Connection, database: db})

      assert_raise RuntimeError, fn ->
        Connection.query_pointer(conn, "SELECT 1", fn _pointer ->
          raise "oops"
        end)
      end

      run_anything(conn)
    end

    test "broken link releases the lock", %{db: db} do
      conn = start_supervised!({Connection, database: db})
      parent = self()

      child =
        spawn(fn ->
          Connection.query_pointer(conn, "SELECT 1", fn _pointer ->
            send(parent, :ready)
            Process.sleep(:infinity)
          end)
        end)

      assert_receive :ready
      Process.exit(child, :kill)
      run_anything(conn)
    end

    test "commands that error do not lock", %{db: db} do
      conn = start_supervised!({Connection, database: db})
      {:error, %Adbc.Error{}} = Connection.query(conn, "NOT VALID SQL")
      run_anything(conn)
    end

    defp run_anything(conn) do
      {:ok, %{}} = Connection.get_table_types(conn)
    end
  end

  describe "transactions" do
    @tag skip: "needs to start a transaction"
    test "rollback" do
    end

    @tag skip: "needs to start a transaction"
    test "commit" do
    end
  end
end
