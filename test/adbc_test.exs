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
              "unknown driver :unknown, expected one of :duckdb, :flightsql, :postgresql, " <> _} =
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
      assert {:ok,
              %Adbc.Result{
                data: [
                  %Adbc.Column{
                    name: "num",
                    type: :i32,
                    nullable: true,
                    metadata: nil,
                    data: [123]
                  }
                ]
              }} =
               Connection.query(conn, "SELECT 123 as num")
    end

    test "list responses", %{conn: conn} do
      assert {:ok,
              %Adbc.Result{
                data: [
                  %Adbc.Column{
                    name: "num",
                    type: :list,
                    nullable: true,
                    metadata: nil,
                    data: [
                      %Adbc.Column{
                        name: "item",
                        type: :i32,
                        nullable: true,
                        metadata: nil,
                        data: [1, 2, 3]
                      }
                    ]
                  }
                ]
              }} =
               Connection.query(conn, "SELECT ARRAY[1, 2, 3] as num")
    end

    test "list responses with null", %{conn: conn} do
      assert {:ok,
              %Adbc.Result{
                data: [
                  %Adbc.Column{
                    name: "num",
                    type: :list,
                    nullable: true,
                    metadata: nil,
                    data: [
                      %Adbc.Column{
                        name: "item",
                        type: :i32,
                        nullable: true,
                        metadata: nil,
                        data: [1, 2, 3, nil, 5]
                      }
                    ]
                  }
                ]
              }} =
               Connection.query(conn, "SELECT ARRAY[1, 2, 3, null, 5] as num")
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
      assert {:ok,
              %Adbc.Result{
                data: [
                  %Adbc.Column{
                    name: "num",
                    type: :list,
                    nullable: true,
                    metadata: nil,
                    data: [
                      %Adbc.Column{
                        name: "item",
                        type: :i32,
                        nullable: true,
                        metadata: nil,
                        data: [1, 2, 3, nil, 5, 6, nil, 7, nil, 9]
                      }
                    ]
                  }
                ]
              }} =
               Connection.query(
                 conn,
                 "SELECT ARRAY[ARRAY[1, 2, 3, null, 5], ARRAY[6, null, 7, null, 9]] as num"
               )
    end

    test "getting all chunks", %{conn: conn} do
      query = """
      SELECT * FROM generate_series('2000-03-01 00:00'::timestamp, '2100-03-04 12:00'::timestamp, '15 minutes')
      """

      %Adbc.Result{
        data: [
          %Adbc.Column{
            name: "generate_series",
            type: {:timestamp, :microseconds, nil},
            nullable: true,
            metadata: nil,
            data: generate_series
          }
        ]
      } = Connection.query!(conn, query)

      assert Enum.count(generate_series) == 3_506_641
    end

    test "select with temporal types", %{conn: conn} do
      query = """
      select
        '2023-03-01T10:23:45'::timestamp as datetime,
        '2023-03-01T10:23:45.123456'::timestamp as datetime_usec,
        '2023-03-01T10:23:45 PST'::timestamptz as datetime_tz,
        '2023-03-01'::date as date,
        '10:23:45'::time as time,
        '10:23:45.123456'::time as time_usec
      """

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
                   name: "datetime_tz",
                   type: {:timestamp, :microseconds, "UTC"},
                   nullable: true,
                   metadata: nil,
                   data: [~N[2023-03-01 18:23:45.000000]]
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
             } = Connection.query!(conn, query)
    end
  end

  describe "duckdb smoke tests" do
    test "starts a database" do
      assert {:ok, _} = Database.start_link(driver: :duckdb)
    end
  end
end
