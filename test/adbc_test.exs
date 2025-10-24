defmodule AdbcTest do
  use ExUnit.Case, async: true
  doctest Adbc

  alias Adbc.{Database, Connection}

  describe "download_driver" do
    test "multiple times" do
      assert Adbc.download_driver(:sqlite) == :ok
      assert Adbc.download_driver(:sqlite) == :ok
    end

    test "returns errors" do
      assert {:error,
              "unknown driver :unknown, expected one of :bigquery, :duckdb, :flightsql, :postgresql, " <>
                _} =
               Adbc.download_driver(:unknown)
    end
  end

  describe "postgresql smoke tests" do
    @describetag :postgresql

    setup do
      db =
        start_supervised!(
          {Database, driver: :postgresql, uri: "postgres://postgres:postgres@localhost"}
        )

      conn = start_supervised!({Connection, database: db})

      %{db: db, conn: conn}
    end

    test "runs queries", %{conn: conn} do
      assert {:ok, results} = Connection.query(conn, "SELECT 123 as num")

      assert %Adbc.Result{
               data: [
                 %Adbc.Column{
                   name: "num",
                   type: :s32,
                   nullable: true,
                   metadata: nil,
                   data: [123]
                 }
               ]
             } = Adbc.Result.materialize(results)
    end

    test "list responses", %{conn: conn} do
      assert {:ok, results} = Connection.query(conn, "SELECT ARRAY[1, 2, 3] as num")

      assert %Adbc.Result{
               data: [
                 %Adbc.Column{
                   name: "num",
                   type: :list,
                   nullable: true,
                   metadata: nil,
                   data: [
                     %Adbc.Column{
                       name: "item",
                       type: :s32,
                       nullable: true,
                       metadata: nil,
                       data: [1, 2, 3]
                     }
                   ]
                 }
               ]
             } = Adbc.Result.materialize(results)
    end

    test "list responses with null", %{conn: conn} do
      assert {:ok, results} = Connection.query(conn, "SELECT ARRAY[1, 2, 3, null, 5] as num")

      assert %Adbc.Result{
               data: [
                 %Adbc.Column{
                   name: "num",
                   type: :list,
                   nullable: true,
                   metadata: nil,
                   data: [
                     %Adbc.Column{
                       name: "item",
                       type: :s32,
                       nullable: true,
                       metadata: nil,
                       data: [1, 2, 3, nil, 5]
                     }
                   ]
                 }
               ]
             } = Adbc.Result.materialize(results)
    end

    test "nested list responses with null", %{conn: conn} do
      # import adbc_driver_postgresql
      # import adbc_driver_manager
      # import pyarrow
      # db = adbc_driver_postgresql.connect(uri="postgres://postgres:postgres@localhost"):
      # conn = adbc_driver_manager.AdbcConnection(db)
      # stmt = adbc_driver_manager.AdbcStatement(conn)
      # stmt.set_sql_query("SELECT ARRAY[ARRAY[1, 2, 3, null, 5], ARRAY[6, null, 7, null, 9]] as num")
      # stream, _ = stmt.execute_query()
      # reader = pyarrow.RecordBatchReader._import_from_c(stream.address)
      # print(reader.read_all())
      #
      # pyarrow.Table
      # num: list<item: int32>
      #   child 0, item: int32
      # ----
      # num: [[[1,2,3,null,5,6,null,7,null,9]]]
      assert {:ok, results} =
               Connection.query(
                 conn,
                 "SELECT ARRAY[ARRAY[1, 2, 3, null, 5], ARRAY[6, null, 7, null, 9]] as num"
               )

      assert %Adbc.Result{
               data: [
                 %Adbc.Column{
                   name: "num",
                   type: :list,
                   nullable: true,
                   metadata: nil,
                   data: [
                     %Adbc.Column{
                       name: "item",
                       type: :s32,
                       nullable: true,
                       metadata: nil,
                       data: [1, 2, 3, nil, 5, 6, nil, 7, nil, 9]
                     }
                   ]
                 }
               ]
             } = Adbc.Result.materialize(results)
    end

    test "getting all chunks", %{conn: conn} do
      query = """
      SELECT * FROM generate_series('2000-03-01 00:00'::timestamp, '2100-03-04 12:00'::timestamp, '15 minutes')
      """

      assert results =
               %Adbc.Result{
                 data: [
                   %Adbc.Column{
                     name: "generate_series",
                     type: {:timestamp, :microseconds, nil},
                     metadata: nil,
                     nullable: true
                   }
                 ]
               } = Connection.query!(conn, query)

      assert %Adbc.Result{
               data: [
                 %Adbc.Column{
                   name: "generate_series",
                   type: {:timestamp, :microseconds, nil},
                   nullable: true,
                   metadata: nil,
                   data: generate_series
                 }
               ]
             } = Adbc.Result.materialize(results)

      assert Enum.count(generate_series) == 3_506_641
    end

    test "select with temporal types", %{conn: conn} do
      query = """
      select
        '2023-03-01T10:23:45'::timestamp as datetime,
        '2023-03-01T10:23:45.123456'::timestamp as datetime_usec,
        '2023-03-01T10:23:45 PST'::timestamptz as datetime_tz_8601,
        '2023-03-01T10:23:45+02'::timestamptz as datetime_tz_offset,
        '2023-03-01'::date as date,
        '10:23:45'::time as time,
        '10:23:45.123456'::time as time_usec
      """

      assert {:ok, results} = Connection.query(conn, query)

      assert %Adbc.Result{
               data: [
                 %Adbc.Column{
                   name: "datetime",
                   type: {:timestamp, :microseconds, nil},
                   nullable: true,
                   metadata: nil,
                   data: [~N[2023-03-01 10:23:45.000000]]
                 },
                 %Adbc.Column{
                   name: "datetime_usec",
                   type: {:timestamp, :microseconds, nil},
                   nullable: true,
                   metadata: nil,
                   data: [~N[2023-03-01 10:23:45.123456]]
                 },
                 %Adbc.Column{
                   name: "datetime_tz_8601",
                   type: {:timestamp, :microseconds, "UTC"},
                   nullable: true,
                   metadata: nil,
                   data: [~N[2023-03-01 18:23:45.000000]]
                 },
                 %Adbc.Column{
                   name: "datetime_tz_offset",
                   type: {:timestamp, :microseconds, "UTC"},
                   nullable: true,
                   metadata: nil,
                   data: [~N[2023-03-01 08:23:45.000000]]
                 },
                 %Adbc.Column{
                   name: "date",
                   type: :date32,
                   nullable: true,
                   metadata: nil,
                   data: [~D[2023-03-01]]
                 },
                 %Adbc.Column{
                   name: "time",
                   type: {:time64, :microseconds},
                   nullable: true,
                   metadata: nil,
                   data: [~T[10:23:45.000000]]
                 },
                 %Adbc.Column{
                   name: "time_usec",
                   type: {:time64, :microseconds},
                   nullable: true,
                   metadata: nil,
                   data: [~T[10:23:45.123456]]
                 }
               ]
             } = Adbc.Result.materialize(results)
    end

    test "inf/-inf/nan", %{db: _, conn: conn} do
      assert {:ok, results} =
               Adbc.Connection.query(
                 conn,
                 "SELECT ARRAY['infinity'::NUMERIC, '-infinity'::NUMERIC, 4.2::NUMERIC, 'nan'::NUMERIC];"
               )

      assert %Adbc.Result{
               data: [
                 %Adbc.Column{
                   data: [
                     %Adbc.Column{
                       name: "item",
                       type: :string,
                       nullable: true,
                       metadata: %{
                         "ADBC:postgresql:typname" => "numeric"
                       },
                       data: ["inf", "-inf", "4.2", "nan"]
                     }
                   ],
                   metadata: nil,
                   name: "array",
                   nullable: true,
                   type: :list
                 }
               ]
             } = Adbc.Result.materialize(results)
    end

    test "query with parameters", %{db: _, conn: conn} do
      assert {:ok, result} =
               Adbc.Connection.query(
                 conn,
                 "SELECT $1 as x",
                 [Adbc.Column.s32([1, 2, 3])]
               )

      assert %Adbc.Result{
               data: [
                 %Adbc.Column{
                   data: [1, 2, 3],
                   name: "x",
                   type: :s32,
                   metadata: nil,
                   nullable: true
                 }
               ]
             } = result |> Adbc.Result.materialize()
    end

    test "query with parameters, operator in", %{db: _, conn: conn} do
      values = [1, 2, 3]
      not_in_values = 4

      for v <- values do
        assert {:ok, result} =
                 Adbc.Connection.query(
                   conn,
                   "SELECT ($2 = ANY($1))::int",
                   [Adbc.Column.list([Adbc.Column.s32(values)]), Adbc.Column.s32([v])]
                 )

        assert %Adbc.Result{
                 data: [
                   %Adbc.Column{
                     data: [1],
                     name: "int4",
                     type: :s32,
                     metadata: nil,
                     nullable: true
                   }
                 ]
               } = result |> Adbc.Result.materialize()
      end

      refute Enum.member?(values, not_in_values)

      assert {:ok, result} =
               Adbc.Connection.query(
                 conn,
                 "SELECT ($2 = ANY($1))::int",
                 [Adbc.Column.list([Adbc.Column.s32(values)]), Adbc.Column.s32([not_in_values])]
               )

      assert %Adbc.Result{
               data: [
                 %Adbc.Column{
                   data: [0],
                   name: "int4",
                   type: :s32,
                   metadata: nil,
                   nullable: true
                 }
               ]
             } = result |> Adbc.Result.materialize()
    end

    test "top-level parameter values should have the same length/rows", %{db: _, conn: conn} do
      values = [1, 2, 3]
      not_in_values = 4

      assert_raise ArgumentError,
                   "Expected struct child 2 to have length >= 3 but found child with length 1",
                   fn ->
                     Adbc.Connection.query!(
                       conn,
                       "SELECT ($2 = ANY($1))::int",
                       [Adbc.Column.s32(values), Adbc.Column.s32([not_in_values])]
                     )
                   end
    end

    test "list of strings", %{db: _, conn: conn} do
      ids = ["1", "2", "3"]

      assert {:ok, result} =
               Adbc.Connection.query(
                 conn,
                 "SELECT $1",
                 [Adbc.Column.list([Adbc.Column.string(ids)])]
               )

      assert %Adbc.Result{
               data: [
                 %Adbc.Column{
                   data: [
                     %Adbc.Column{
                       data: ["1", "2", "3"],
                       name: "item",
                       type: :string,
                       metadata: nil,
                       nullable: true
                     }
                   ],
                   type: :list,
                   metadata: nil,
                   nullable: true
                 }
               ]
             } = result |> Adbc.Result.materialize()
    end
  end

  describe "duckdb smoke tests" do
    @describetag :duckdb

    setup do
      db = start_supervised!({Database, driver: :duckdb})
      conn = start_supervised!({Connection, database: db})

      %{db: db, conn: conn}
    end

    test "error", %{conn: conn} do
      assert {:error, %Adbc.Error{} = error} =
               Adbc.Connection.query(conn, "SELECT * from $1", ["foo"])

      assert Exception.message(error) =~ "Parser Error"
    end

    test "decimal128", %{conn: conn} do
      d1 = Decimal.new("1.2345678912345678912345678912345678912")
      d2 = Decimal.new("-1.2345678912345678912345678912345678912")
      d5 = Decimal.new("9876543210987654321098765432109876543.2")
      d6 = Decimal.new("-9876543210987654321098765432109876543.2")
      d7 = Decimal.new("1E-37")

      assert {:ok,
              results = %Adbc.Result{
                data: [
                  %Adbc.Column{
                    name: "d1",
                    type: {:decimal, 128, 38, 37},
                    metadata: nil,
                    nullable: true
                  },
                  %Adbc.Column{
                    name: "d2",
                    type: {:decimal, 128, 38, 37},
                    metadata: nil,
                    nullable: true
                  },
                  %Adbc.Column{
                    name: "d3",
                    type: :f64,
                    metadata: nil,
                    nullable: true
                  },
                  %Adbc.Column{
                    name: "d4",
                    type: :f64,
                    metadata: nil,
                    nullable: true
                  },
                  %Adbc.Column{
                    name: "d5",
                    type: {:decimal, 128, 38, 1},
                    metadata: nil,
                    nullable: true
                  },
                  %Adbc.Column{
                    name: "d6",
                    type: {:decimal, 128, 38, 1},
                    metadata: nil,
                    nullable: true
                  },
                  %Adbc.Column{
                    name: "d7",
                    type: {:decimal, 128, 38, 37},
                    metadata: nil,
                    nullable: true
                  }
                ]
              }} =
               Connection.query(
                 conn,
                 """
                 SELECT
                 1.2345678912345678912345678912345678912 as d1,
                 -1.2345678912345678912345678912345678912 as d2,
                 1.23456789123456789123456789123456789123 as d3,
                 -1.23456789123456789123456789123456789123 as d4,
                 9876543210987654321098765432109876543.2 as d5,
                 -9876543210987654321098765432109876543.2 as d6,
                 0.0000000000000000000000000000000000001 as d7,
                 """
               )

      assert %Adbc.Result{
               data: [
                 %Adbc.Column{
                   name: "d1",
                   type: {:decimal, 128, 38, 37},
                   nullable: true,
                   metadata: nil,
                   data: [^d1]
                 },
                 %Adbc.Column{
                   name: "d2",
                   type: {:decimal, 128, 38, 37},
                   nullable: true,
                   metadata: nil,
                   data: [^d2]
                 },
                 %Adbc.Column{
                   name: "d3",
                   type: :f64,
                   nullable: true,
                   metadata: nil,
                   data: [1.234567891234568]
                 },
                 %Adbc.Column{
                   name: "d4",
                   type: :f64,
                   nullable: true,
                   metadata: nil,
                   data: [-1.234567891234568]
                 },
                 %Adbc.Column{
                   name: "d5",
                   type: {:decimal, 128, 38, 1},
                   nullable: true,
                   metadata: nil,
                   data: [^d5]
                 },
                 %Adbc.Column{
                   name: "d6",
                   type: {:decimal, 128, 38, 1},
                   nullable: true,
                   metadata: nil,
                   data: [^d6]
                 },
                 %Adbc.Column{
                   name: "d7",
                   type: {:decimal, 128, 38, 37},
                   nullable: true,
                   metadata: nil,
                   data: [^d7]
                 }
               ]
             } = Adbc.Result.materialize(results)
    end
  end
end
