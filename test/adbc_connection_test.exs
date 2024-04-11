defmodule Adbc.Connection.Test do
  use ExUnit.Case
  doctest Adbc.Connection

  alias Adbc.Connection

  setup do
    db = start_supervised!({Adbc.Database, driver: :sqlite, uri: ":memory:"})
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

      assert Exception.message(error) == "[SQLite] Unknown connection option who_knows='123'"
    end
  end

  describe "get_info" do
    test "get all info from a connection", %{db: db} do
      conn = start_supervised!({Connection, database: db})

      {:ok,
       %Adbc.Result{
         num_rows: nil,
         data: %{
           "info_name" => [0, 1, 100, 101, 102],
           "info_value" => [
             %{"string_value" => ["SQLite"]},
             # %{"string_value" => ["3.43.2"]},
             %{"string_value" => [_]},
             %{"string_value" => ["ADBC SQLite Driver"]},
             # %{"string_value" => ["(unknown)"]},
             %{"string_value" => [_]},
             # %{"string_value" => ["0.4.0"]}
             %{"string_value" => [_]}
           ]
         }
       }} = Connection.get_info(conn)
    end

    test "get some info from a connection", %{db: db} do
      conn = start_supervised!({Connection, database: db})

      assert {:ok,
              %Adbc.Result{
                num_rows: nil,
                data: %{
                  "info_name" => [0],
                  "info_value" => [%{"string_value" => ["SQLite"]}]
                }
              }} == Connection.get_info(conn, [0])
    end
  end

  describe "get_driver" do
    test "returns the driver", %{db: db} do
      conn = start_supervised!({Connection, database: db})
      assert Connection.get_driver(conn) == {:ok, :sqlite}
    end

    test "returns :error for non ADBC connection" do
      assert Connection.get_driver(self()) == :error
    end

    test "returns :error for dead process" do
      assert Connection.get_driver(:not_really_a_process) == :error
    end
  end

  describe "get_objects" do
    test "get all objects from a connection", %{db: db} do
      conn = start_supervised!({Connection, database: db})

      {:ok,
       %Adbc.Result{
         num_rows: nil,
         data: %{
           "catalog_db_schemas" => [
             {"db_schema_name", []},
             {"db_schema_tables",
              [
                {"table_name", []},
                {"table_type", []},
                {"table_columns",
                 [
                   {"column_name", []},
                   {"ordinal_position", []},
                   {"remarks", []},
                   {"xdbc_data_type", []},
                   {"xdbc_type_name", []},
                   {"xdbc_column_size", []},
                   {"xdbc_decimal_digits", []},
                   {"xdbc_num_prec_radix", []},
                   {"xdbc_nullable", []},
                   {"xdbc_column_def", []},
                   {"xdbc_sql_data_type", []},
                   {"xdbc_datetime_sub", []},
                   {"xdbc_char_octet_length", []},
                   {"xdbc_is_nullable", []},
                   {"xdbc_scope_catalog", []},
                   {"xdbc_scope_schema", []},
                   {"xdbc_scope_table", []},
                   {"xdbc_is_autoincrement", []},
                   {"xdbc_is_generatedcolumn", []}
                 ]},
                {"table_constraints",
                 [
                   {"constraint_name", []},
                   {"constraint_type", []},
                   {"constraint_column_names", [[]]},
                   {"constraint_column_usage",
                    [
                      {"fk_catalog", []},
                      {"fk_db_schema", []},
                      {"fk_table", []},
                      {"fk_column_name", []}
                    ]}
                 ]}
              ]}
           ],
           "catalog_name" => []
         }
       }} = Connection.get_objects(conn, 0)
    end
  end

  describe "get_table_types" do
    test "get table types from a connection", %{db: db} do
      conn = start_supervised!({Connection, database: db})

      assert {:ok, %Adbc.Result{data: %{"table_type" => ["table", "view"]}}} =
               Connection.get_table_types(conn)
    end
  end

  describe "query" do
    test "select", %{db: db} do
      conn = start_supervised!({Connection, database: db})

      assert {:ok, %Adbc.Result{data: %{"num" => [123]}}} =
               Connection.query(conn, "SELECT 123 as num")

      assert {:ok, %Adbc.Result{data: %{"num" => [123], "bool" => [1]}}} =
               Connection.query(conn, "SELECT 123 as num, true as bool")
    end

    test "select with parameters", %{db: db} do
      conn = start_supervised!({Connection, database: db})

      assert {:ok, %Adbc.Result{data: %{"num" => [579]}}} =
               Connection.query(conn, "SELECT 123 + ? as num", [456])
    end

    test "fails on invalid query", %{db: db} do
      conn = start_supervised!({Connection, database: db})
      assert {:error, %Adbc.Error{} = error} = Connection.query(conn, "NOT VALID SQL")
      assert Exception.message(error) =~ "[SQLite] Failed to prepare query"
    end

    test "select with prepared query", %{db: db} do
      conn = start_supervised!({Connection, database: db})
      assert {:ok, ref} = Connection.prepare(conn, "SELECT 123 + ? as num")

      assert {:ok, %Adbc.Result{data: %{"num" => [579]}}} =
               Connection.query(conn, ref, [456])
    end

    test "select with multiple prepared queries", %{db: db} do
      conn = start_supervised!({Connection, database: db})
      assert {:ok, ref_a} = Connection.prepare(conn, "SELECT 123 + ? as num")
      assert {:ok, ref_b} = Connection.prepare(conn, "SELECT 1000 + ? as num")

      assert {:ok, %Adbc.Result{data: %{"num" => [579]}}} =
               Connection.query(conn, ref_a, [456])

      assert {:ok, %Adbc.Result{data: %{"num" => [1456]}}} =
               Connection.query(conn, ref_b, [456])
    end
  end

  describe "query!" do
    test "select", %{db: db} do
      conn = start_supervised!({Connection, database: db})

      assert %Adbc.Result{data: %{"num" => [123]}} =
               Connection.query!(conn, "SELECT 123 as num")

      assert %Adbc.Result{data: %{"num" => [123], "bool" => [1]}} =
               Connection.query!(conn, "SELECT 123 as num, true as bool")
    end

    test "select with parameters", %{db: db} do
      conn = start_supervised!({Connection, database: db})

      assert %Adbc.Result{data: %{"num" => [579]}} =
               Connection.query!(conn, "SELECT 123 + ? as num", [456])
    end

    test "fails on invalid query", %{db: db} do
      conn = start_supervised!({Connection, database: db})

      assert_raise Adbc.Error,
                   ~s([SQLite] Failed to prepare query: near "NOT": syntax error\nquery: NOT VALID SQL),
                   fn -> Connection.query!(conn, "NOT VALID SQL") end
    end
  end

  describe "prepared queries" do
    test "prepare", %{db: db} do
      conn = start_supervised!({Connection, database: db})
      assert {:ok, ref} = Connection.prepare(conn, "SELECT 123 + ? as num")
      assert is_reference(ref)
    end
  end

  describe "query_pointer" do
    test "select", %{db: db} do
      conn = start_supervised!({Connection, database: db})

      assert {:ok, :from_pointer} =
               Connection.query_pointer(conn, "SELECT 123 as num", fn
                 pointer, nil when is_integer(pointer) ->
                   :from_pointer
               end)
    end

    test "prepared query", %{db: db} do
      conn = start_supervised!({Connection, database: db})

      {:ok, ref} = Connection.prepare(conn, "SELECT 123 + ? as num")

      assert {:ok, :from_pointer} =
               Connection.query_pointer(conn, ref, [456], fn
                 pointer, nil when is_integer(pointer) ->
                   :from_pointer
               end)
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
        Connection.query_pointer(conn, "SELECT 1", fn _pointer, _num_rows ->
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
          Connection.query_pointer(conn, "SELECT 1", fn _pointer, _num_rows ->
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
      {:error, %Adbc.Error{}} = Connection.prepare(conn, "NOT VALID SQL")
      run_anything(conn)
    end

    defp run_anything(conn) do
      {:ok, %{}} = Connection.get_table_types(conn)
    end
  end
end
