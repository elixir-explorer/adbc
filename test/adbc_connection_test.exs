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

      assert Exception.message(error) == "[SQLite] Unknown connection option who_knows=123"
    end
  end

  describe "get_info" do
    test "get all info from a connection", %{db: db} do
      conn = start_supervised!({Connection, database: db})

      assert {
               :ok,
               results = %Adbc.Result{
                 data: %Adbc.Column{
                   data: _,
                   name: "",
                   type:
                     {:struct,
                      [
                        %Adbc.Column{
                          name: "info_name",
                          type: :u32,
                          metadata: nil,
                          nullable: false
                        },
                        %Adbc.Column{
                          name: "info_value",
                          type: :dense_union,
                          metadata: nil,
                          nullable: true
                        }
                      ]},
                   metadata: nil,
                   nullable: true
                 },
                 num_rows: nil
               }
             } = Connection.get_info(conn)

      assert %Adbc.Result{
               num_rows: nil,
               data: [
                 %Adbc.Column{
                   name: "info_name",
                   type: :u32,
                   nullable: false,
                   metadata: nil,
                   data: [0, 1, 100, 101, 102]
                 },
                 %Adbc.Column{
                   name: "info_value",
                   type: :dense_union,
                   nullable: true,
                   metadata: nil,
                   data: [
                     %{"string_value" => ["SQLite"]},
                     # "3.43.2"
                     %{"string_value" => [_]},
                     %{"string_value" => ["ADBC SQLite Driver"]},
                     # "(unknown)"
                     %{"string_value" => [_]},
                     # "0.4.0"
                     %{"string_value" => [_]}
                   ]
                 }
               ]
             } = Adbc.Result.materialize(results)
    end

    test "get some info from a connection", %{db: db} do
      conn = start_supervised!({Connection, database: db})

      assert {:ok,
              results = %Adbc.Result{
                data: %Adbc.Column{
                  data: _,
                  name: "",
                  type:
                    {:struct,
                     [
                       %Adbc.Column{
                         name: "info_name",
                         type: :u32,
                         metadata: nil,
                         nullable: false
                       },
                       %Adbc.Column{
                         name: "info_value",
                         type: :dense_union,
                         metadata: nil,
                         nullable: true
                       }
                     ]},
                  metadata: nil,
                  nullable: true
                },
                num_rows: nil
              }} = Connection.get_info(conn, [0])

      assert %Adbc.Result{
               data: [
                 %Adbc.Column{
                   name: "info_name",
                   type: :u32,
                   nullable: false,
                   metadata: nil,
                   data: [0]
                 },
                 %Adbc.Column{
                   name: "info_value",
                   type: :dense_union,
                   nullable: true,
                   metadata: nil,
                   data: [%{"string_value" => ["SQLite"]}]
                 }
               ]
             } = Adbc.Result.materialize(results)
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

      assert {:ok,
              results = %Adbc.Result{
                num_rows: nil,
                data: _
              }} = Connection.get_objects(conn, 0)

      assert results =
               %Adbc.Result{
                 num_rows: nil,
                 data: [
                   %Adbc.Column{
                     data: [],
                     name: "catalog_name",
                     type: :string,
                     metadata: nil,
                     nullable: true
                   },
                   %Adbc.Column{
                     data: [],
                     name: "catalog_db_schemas",
                     type: :list,
                     metadata: nil,
                     nullable: true
                   }
                 ]
               } = Adbc.Result.materialize(results)

      assert %{"catalog_db_schemas" => [], "catalog_name" => []} == Adbc.Result.to_map(results)
    end
  end

  describe "get_table_types" do
    test "get table types from a connection", %{db: db} do
      conn = start_supervised!({Connection, database: db})

      assert {:ok,
              results = %Adbc.Result{
                num_rows: nil,
                data: %Adbc.Column{
                  data: _,
                  name: "",
                  type:
                    {:struct,
                     [
                       %Adbc.Column{
                         name: "table_type",
                         type: :string,
                         metadata: nil,
                         nullable: false
                       }
                     ]},
                  metadata: nil,
                  nullable: true
                }
              }} = Connection.get_table_types(conn)

      assert results =
               %Adbc.Result{
                 data: [
                   %Adbc.Column{
                     name: "table_type",
                     type: :string,
                     nullable: false,
                     metadata: nil,
                     data: ["table", "view"]
                   }
                 ]
               } = Adbc.Result.materialize(results)

      assert %{"table_type" => ["table", "view"]} = Adbc.Result.to_map(results)
    end
  end

  describe "query" do
    test "select", %{db: db} do
      conn = start_supervised!({Connection, database: db})

      assert {:ok,
              results = %Adbc.Result{
                data: %Adbc.Column{
                  data: _,
                  name: "",
                  type:
                    {:struct,
                     [
                       %Adbc.Column{
                         name: "num",
                         type: :s64,
                         metadata: nil,
                         nullable: true
                       }
                     ]},
                  metadata: nil,
                  nullable: true
                },
                num_rows: nil
              }} = Connection.query(conn, "SELECT 123 as num")

      assert %Adbc.Result{
               data: [
                 %Adbc.Column{
                   name: "num",
                   type: :s64,
                   nullable: true,
                   metadata: nil,
                   data: [123]
                 }
               ]
             } = Adbc.Result.materialize(results)

      assert {:ok,
              results = %Adbc.Result{
                data: %Adbc.Column{
                  data: _,
                  name: "",
                  type:
                    {:struct,
                     [
                       %Adbc.Column{
                         name: "num",
                         type: :s64,
                         metadata: nil,
                         nullable: true
                       },
                       %Adbc.Column{
                         name: "bool",
                         type: :s64,
                         metadata: nil,
                         nullable: true
                       }
                     ]},
                  metadata: nil,
                  nullable: true
                },
                num_rows: nil
              }} = Connection.query(conn, "SELECT 123 as num, true as bool")

      assert %Adbc.Result{
               data: [
                 %Adbc.Column{
                   name: "num",
                   type: :s64,
                   nullable: true,
                   metadata: nil,
                   data: [123]
                 },
                 %Adbc.Column{
                   name: "bool",
                   type: :s64,
                   nullable: true,
                   metadata: nil,
                   data: [1]
                 }
               ]
             } = Adbc.Result.materialize(results)
    end

    test "select with parameters", %{db: db} do
      conn = start_supervised!({Connection, database: db})

      assert {:ok,
              results = %Adbc.Result{
                data: %Adbc.Column{
                  data: _,
                  name: "",
                  type:
                    {:struct,
                     [
                       %Adbc.Column{
                         name: "num",
                         type: :s64,
                         metadata: nil,
                         nullable: true
                       }
                     ]},
                  metadata: nil,
                  nullable: true
                },
                num_rows: nil
              }} = Connection.query(conn, "SELECT 123 + ? as num", [456])

      assert %Adbc.Result{
               data: [
                 %Adbc.Column{
                   name: "num",
                   type: :s64,
                   nullable: true,
                   metadata: nil,
                   data: [579]
                 }
               ]
             } = Adbc.Result.materialize(results)
    end

    test "use reference type results as query parameter", %{db: db} do
      conn = start_supervised!({Connection, database: db})

      assert {:ok, results = %Adbc.Result{data: column}} =
               Connection.query(conn, "SELECT 123 + ? as num", [456])

      assert %Adbc.Result{
               data: [
                 %Adbc.Column{
                   name: "num",
                   type: :s64,
                   nullable: true,
                   metadata: nil,
                   data: [579]
                 }
               ]
             } = Adbc.Result.materialize(results)

      assert {:ok,
              results = %Adbc.Result{
                data: _,
                num_rows: nil
              }} = Connection.query(conn, "SELECT ? + ? as num", [421, column])

      assert %Adbc.Result{
               data: [
                 %Adbc.Column{
                   name: "num",
                   type: :s64,
                   nullable: true,
                   metadata: nil,
                   data: [1000]
                 }
               ]
             } = Adbc.Result.materialize(results)
    end

    test "fails on invalid query", %{db: db} do
      conn = start_supervised!({Connection, database: db})
      assert {:error, %Adbc.Error{} = error} = Connection.query(conn, "NOT VALID SQL")
      assert Exception.message(error) =~ "[SQLite] Failed to prepare query"
    end

    test "select with prepared query", %{db: db} do
      conn = start_supervised!({Connection, database: db})
      assert {:ok, ref} = Connection.prepare(conn, "SELECT 123 + ? as num")

      assert {:ok,
              results = %Adbc.Result{
                data: %Adbc.Column{
                  data: _,
                  name: "",
                  type:
                    {:struct,
                     [
                       %Adbc.Column{
                         name: "num",
                         type: :s64,
                         metadata: nil,
                         nullable: true
                       }
                     ]},
                  metadata: nil,
                  nullable: true
                },
                num_rows: nil
              }} = Connection.query(conn, ref, [456])

      assert %Adbc.Result{
               data: [
                 %Adbc.Column{
                   name: "num",
                   type: :s64,
                   nullable: true,
                   metadata: nil,
                   data: [579]
                 }
               ]
             } = Adbc.Result.materialize(results)
    end

    test "select with multiple prepared queries", %{db: db} do
      conn = start_supervised!({Connection, database: db})
      assert {:ok, ref_a} = Connection.prepare(conn, "SELECT 123 + ? as num")
      assert {:ok, ref_b} = Connection.prepare(conn, "SELECT 1000 + ? as num")

      assert {:ok,
              results = %Adbc.Result{
                data: %Adbc.Column{
                  data: _,
                  name: "",
                  type:
                    {:struct,
                     [
                       %Adbc.Column{
                         name: "num",
                         type: :s64,
                         metadata: nil,
                         nullable: true
                       }
                     ]},
                  metadata: nil,
                  nullable: true
                },
                num_rows: nil
              }} = Connection.query(conn, ref_a, [456])

      assert %Adbc.Result{
               data: [
                 %Adbc.Column{
                   name: "num",
                   type: :s64,
                   nullable: true,
                   metadata: nil,
                   data: [579]
                 }
               ]
             } = Adbc.Result.materialize(results)

      assert {:ok,
              results = %Adbc.Result{
                data: %Adbc.Column{
                  data: _,
                  name: "",
                  type:
                    {:struct,
                     [
                       %Adbc.Column{
                         name: "num",
                         type: :s64,
                         metadata: nil,
                         nullable: true
                       }
                     ]},
                  metadata: nil,
                  nullable: true
                },
                num_rows: nil
              }} = Connection.query(conn, ref_b, [456])

      assert %Adbc.Result{
               data: [
                 %Adbc.Column{
                   name: "num",
                   type: :s64,
                   nullable: true,
                   metadata: nil,
                   data: [1456]
                 }
               ]
             } = Adbc.Result.materialize(results)
    end
  end

  describe "query!" do
    test "select", %{db: db} do
      conn = start_supervised!({Connection, database: db})

      assert results =
               %Adbc.Result{
                 data: %Adbc.Column{
                   data: _,
                   name: "",
                   type:
                     {:struct,
                      [
                        %Adbc.Column{
                          name: "num",
                          type: :s64,
                          metadata: nil,
                          nullable: true
                        }
                      ]},
                   metadata: nil,
                   nullable: true
                 },
                 num_rows: nil
               } = Connection.query!(conn, "SELECT 123 as num")

      assert %Adbc.Result{
               data: [
                 %Adbc.Column{
                   name: "num",
                   type: :s64,
                   nullable: true,
                   metadata: nil,
                   data: [123]
                 }
               ]
             } = Adbc.Result.materialize(results)

      assert results =
               %Adbc.Result{
                 data: %Adbc.Column{
                   data: _,
                   name: "",
                   type:
                     {:struct,
                      [
                        %Adbc.Column{
                          name: "num",
                          type: :s64,
                          metadata: nil,
                          nullable: true
                        },
                        %Adbc.Column{
                          name: "bool",
                          type: :s64,
                          metadata: nil,
                          nullable: true
                        }
                      ]},
                   metadata: nil,
                   nullable: true
                 },
                 num_rows: nil
               } = Connection.query!(conn, "SELECT 123 as num, true as bool")

      assert %Adbc.Result{
               data: [
                 %Adbc.Column{
                   name: "num",
                   type: :s64,
                   nullable: true,
                   metadata: nil,
                   data: [123]
                 },
                 %Adbc.Column{name: "bool", type: :s64, nullable: true, metadata: nil, data: [1]}
               ]
             } = Adbc.Result.materialize(results)
    end

    test "select with parameters", %{db: db} do
      conn = start_supervised!({Connection, database: db})

      assert results =
               %Adbc.Result{
                 data: %Adbc.Column{
                   data: _,
                   name: "",
                   type:
                     {:struct,
                      [
                        %Adbc.Column{
                          name: "num",
                          type: :s64,
                          metadata: nil,
                          nullable: true
                        }
                      ]},
                   metadata: nil,
                   nullable: true
                 },
                 num_rows: nil
               } = Connection.query!(conn, "SELECT 123 + ? as num", [456])

      assert %Adbc.Result{
               data: [
                 %Adbc.Column{
                   name: "num",
                   type: :s64,
                   nullable: true,
                   metadata: nil,
                   data: [579]
                 }
               ]
             } = Adbc.Result.materialize(results)
    end

    test "fails on invalid query", %{db: db} do
      conn = start_supervised!({Connection, database: db})

      assert_raise Adbc.Error,
                   ~s([SQLite] Failed to prepare query: near "NOT": syntax error\nquery: NOT VALID SQL),
                   fn -> Connection.query!(conn, "NOT VALID SQL") end
    end
  end

  describe "query with statement options" do
    test "without parameters", %{db: db} do
      conn = start_supervised!({Connection, database: db})

      assert results =
               %Adbc.Result{
                 data: %Adbc.Column{
                   data: _,
                   name: "",
                   type:
                     {:struct,
                      [
                        %Adbc.Column{
                          name: "num",
                          type: :s64,
                          metadata: nil,
                          nullable: true
                        },
                        %Adbc.Column{
                          name: "bool",
                          type: :s64,
                          metadata: nil,
                          nullable: true
                        }
                      ]},
                   metadata: nil,
                   nullable: true
                 },
                 num_rows: nil
               } =
               Connection.query!(conn, "SELECT 123 as num, true as bool", [],
                 "adbc.sqlite.query.batch_rows": 1
               )

      assert %Adbc.Result{
               data: [
                 %Adbc.Column{
                   name: "num",
                   type: :s64,
                   nullable: true,
                   metadata: nil,
                   data: [123]
                 },
                 %Adbc.Column{name: "bool", type: :s64, nullable: true, metadata: nil, data: [1]}
               ]
             } = Adbc.Result.materialize(results)
    end

    test "with parameters", %{db: db} do
      conn = start_supervised!({Connection, database: db})

      assert results =
               %Adbc.Result{
                 data: %Adbc.Column{
                   data: _,
                   name: "",
                   type:
                     {:struct,
                      [
                        %Adbc.Column{
                          name: "num",
                          type: :s64,
                          metadata: nil,
                          nullable: true
                        }
                      ]},
                   metadata: nil,
                   nullable: true
                 },
                 num_rows: nil
               } =
               Connection.query!(conn, "SELECT 123 + ? as num", [456],
                 "adbc.sqlite.query.batch_rows": 10
               )

      assert %Adbc.Result{
               data: [
                 %Adbc.Column{
                   name: "num",
                   type: :s64,
                   nullable: true,
                   metadata: nil,
                   data: [579]
                 }
               ]
             } = Adbc.Result.materialize(results)
    end

    test "invalid statement option key", %{db: db} do
      conn = start_supervised!({Connection, database: db})

      assert {:error, %Adbc.Error{} = error} =
               Connection.query(conn, "SELECT 123 as num", [], foo: 1)

      assert Exception.message(error) == "[SQLite] Unknown statement option foo=1"
    end

    test "invalid statement option value", %{db: db} do
      conn = start_supervised!({Connection, database: db})

      assert {:error, %Adbc.Error{} = error} =
               Connection.query(conn, "SELECT 123 as num, true as bool", [],
                 "adbc.sqlite.query.batch_rows": 0
               )

      assert Exception.message(error) ==
               "[SQLite] Invalid statement option value adbc.sqlite.query.batch_rows=0 (value is non-positive or out of range of int)"
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
