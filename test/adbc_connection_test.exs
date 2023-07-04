defmodule Adbc.Connection.Test do
  use ExUnit.Case
  doctest Adbc.Connection

  alias Adbc.ArrowArrayStream
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
    test "get all info from a connection", %{db: db} do
      conn = start_supervised!({Connection, database: db})

      {:ok, :done} =
        Connection.get_info(conn, fn %ArrowArrayStream{} = array_stream ->
          assert is_reference(array_stream.reference)
          :done
        end)
    end

    test "get some info from a connection", %{db: db} do
      conn = start_supervised!({Connection, database: db})

      {:ok, :done} =
        Connection.get_info(conn, [1], fn %ArrowArrayStream{} = array_stream ->
          assert is_reference(array_stream.reference)
          :done
        end)
    end
  end

  describe "get_objects" do
    test "get all objects from a connection", %{db: db} do
      conn = start_supervised!({Connection, database: db})

      {:ok, :done} =
        Connection.get_objects(conn, 0, fn %ArrowArrayStream{} = array_stream ->
          assert is_reference(array_stream.reference)
          :done
        end)
    end
  end

  describe "get_table_types" do
    test "get table types from a connection", %{db: db} do
      conn = start_supervised!({Connection, database: db})

      {:ok, :done} =
        Connection.get_table_types(conn, fn %ArrowArrayStream{} = array_stream ->
          assert is_reference(array_stream.reference)
          :done
        end)
    end
  end

  describe "query" do
    test "select", %{db: db} do
      conn = start_supervised!({Connection, database: db})

      assert {:ok, %{"num" => [123]}} = Connection.query(conn, "SELECT 123 as num")
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
        Connection.get_table_types(conn, fn %ArrowArrayStream{} ->
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
          Connection.get_table_types(conn, fn %ArrowArrayStream{} ->
            send(parent, :ready)
            Process.sleep(:infinity)
          end)
        end)

      assert_receive :ready
      Process.exit(child, :kill)
      run_anything(conn)
    end

    @tag skip: "needs a command that fails"
    test "commands that error do not lock", %{db: db} do
      conn = start_supervised!({Connection, database: db})

      {:error, %Adbc.Error{}} =
        Connection.get_objects(conn, 0, [table_name: "unknown"], fn %ArrowArrayStream{} ->
          raise "never invoked"
        end)

      run_anything(conn)
    end

    defp run_anything(conn) do
      {:ok, :done} = Connection.get_table_types(conn, fn _ -> :done end)
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
