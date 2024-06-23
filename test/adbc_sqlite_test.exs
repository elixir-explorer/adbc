defmodule Adbc.SQLite.Test do
  use ExUnit.Case

  alias Adbc.Connection

  setup do
    db = start_supervised!({Adbc.Database, driver: :sqlite, uri: ":memory:"})
    conn = start_supervised!({Connection, database: db})

    Connection.query(conn, """
    CREATE TABLE test (
      i1 INT,
      i2 INTEGER,
      i3 TINYINT,
      i4 SMALLINT,
      i5 MEDIUMINT,
      i6 BIGINT,
      i7 UNSIGNED BIG INT,
      i8 INT2,
      i9 INT8,
      t1 CHARACTER(10),
      t2 VARCHAR(10),
      t3 NCHAR(10),
      t4 NVARCHAR(10),
      t5 TEXT,
      t6 CLOB,
      b1 BLOB,
      r1 REAL,
      r2 DOUBLE,
      r3 DOUBLE PRECISION,
      r4 FLOAT,
      n1 NUMERIC,
      n2 DECIMAL(10,5),
      n3 BOOLEAN,
      n4 DATE,
      n5 DATETIME
    );
    """)

    %{db: db, conn: conn}
  end

  test "insert with auto inferred types", %{db: _, conn: conn} do
    Adbc.Connection.query(
      conn,
      """
      INSERT INTO test (i1, i2, i3, i4, i5, i6, i7, i8, i9, t1, t2, t3, t4, t5, t6, b1, r1, r2, r3, r4, n1, n2, n3, n4, n5)
      VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      """,
      [
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        "hello",
        "world",
        "goodbye",
        "world",
        "foo",
        "bar",
        <<"data", 0x01, 0x02>>,
        1.1,
        2.2,
        3.3,
        4.4,
        1.1,
        2.2,
        true,
        "2021-01-01",
        "2021-01-01 00:00:00"
      ]
    )

    assert {:ok, results = %Adbc.Result{num_rows: nil, data: _}} =
             Connection.query(conn, "SELECT * FROM test")

    assert list_result =
             %Adbc.Result{
               data: [
                 %Adbc.Column{name: "i1", type: :s64, nullable: true, metadata: nil, data: [1]},
                 %Adbc.Column{name: "i2", type: :s64, nullable: true, metadata: nil, data: [2]},
                 %Adbc.Column{name: "i3", type: :s64, nullable: true, metadata: nil, data: [3]},
                 %Adbc.Column{name: "i4", type: :s64, nullable: true, metadata: nil, data: [4]},
                 %Adbc.Column{name: "i5", type: :s64, nullable: true, metadata: nil, data: [5]},
                 %Adbc.Column{name: "i6", type: :s64, nullable: true, metadata: nil, data: [6]},
                 %Adbc.Column{
                   name: "i7",
                   type: :s64,
                   nullable: true,
                   metadata: nil,
                   data: ~c"\a"
                 },
                 %Adbc.Column{
                   name: "i8",
                   type: :s64,
                   nullable: true,
                   metadata: nil,
                   data: ~c"\b"
                 },
                 %Adbc.Column{
                   name: "i9",
                   type: :s64,
                   nullable: true,
                   metadata: nil,
                   data: ~c"\t"
                 },
                 %Adbc.Column{
                   name: "t1",
                   type: :string,
                   nullable: true,
                   metadata: nil,
                   data: ["hello"]
                 },
                 %Adbc.Column{
                   name: "t2",
                   type: :string,
                   nullable: true,
                   metadata: nil,
                   data: ["world"]
                 },
                 %Adbc.Column{
                   name: "t3",
                   type: :string,
                   nullable: true,
                   metadata: nil,
                   data: ["goodbye"]
                 },
                 %Adbc.Column{
                   name: "t4",
                   type: :string,
                   nullable: true,
                   metadata: nil,
                   data: ["world"]
                 },
                 %Adbc.Column{
                   name: "t5",
                   type: :string,
                   nullable: true,
                   metadata: nil,
                   data: ["foo"]
                 },
                 %Adbc.Column{
                   name: "t6",
                   type: :string,
                   nullable: true,
                   metadata: nil,
                   data: ["bar"]
                 },
                 %Adbc.Column{
                   name: "b1",
                   type: :string,
                   nullable: true,
                   metadata: nil,
                   data: [<<100, 97, 116, 97, 1, 2>>]
                 },
                 %Adbc.Column{name: "r1", type: :f64, nullable: true, metadata: nil, data: [1.1]},
                 %Adbc.Column{name: "r2", type: :f64, nullable: true, metadata: nil, data: [2.2]},
                 %Adbc.Column{name: "r3", type: :f64, nullable: true, metadata: nil, data: [3.3]},
                 %Adbc.Column{name: "r4", type: :f64, nullable: true, metadata: nil, data: [4.4]},
                 %Adbc.Column{name: "n1", type: :f64, nullable: true, metadata: nil, data: [1.1]},
                 %Adbc.Column{name: "n2", type: :f64, nullable: true, metadata: nil, data: [2.2]},
                 %Adbc.Column{name: "n3", type: :s64, nullable: true, metadata: nil, data: [1]},
                 %Adbc.Column{
                   name: "n4",
                   type: :string,
                   nullable: true,
                   metadata: nil,
                   data: ["2021-01-01"]
                 },
                 %Adbc.Column{
                   name: "n5",
                   type: :string,
                   nullable: true,
                   metadata: nil,
                   data: ["2021-01-01 00:00:00"]
                 }
               ]
             } = Adbc.Result.materialize(results)

    %{
      "b1" => [<<100, 97, 116, 97, 1, 2>>],
      "i1" => [1],
      "i2" => [2],
      "i3" => [3],
      "i4" => [4],
      "i5" => [5],
      "i6" => [6],
      "i7" => ~c"\a",
      "i8" => ~c"\b",
      "i9" => ~c"\t",
      "n1" => [1.1],
      "n2" => [2.2],
      "n3" => [1],
      "n4" => ["2021-01-01"],
      "n5" => ["2021-01-01 00:00:00"],
      "r1" => [1.1],
      "r2" => [2.2],
      "r3" => [3.3],
      "r4" => [4.4],
      "t1" => ["hello"],
      "t2" => ["world"],
      "t3" => ["goodbye"],
      "t4" => ["world"],
      "t5" => ["foo"],
      "t6" => ["bar"]
    } = Adbc.Result.to_map(list_result)
  end

  test "insert with Adbc.Buffer", %{db: _, conn: conn} do
    Connection.query(
      conn,
      """
      INSERT INTO test (i1, i2, i3, i4, i5, i6, i7, i8, i9, t1, t2, t3, t4, t5, t6, b1, r1, r2, r3, r4, n1, n2, n3, n4, n5)
      VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      """,
      [
        Adbc.Column.s8([1]),
        Adbc.Column.s16([2]),
        Adbc.Column.s32([3]),
        Adbc.Column.s64([4]),
        Adbc.Column.u8([5]),
        Adbc.Column.u16([6]),
        Adbc.Column.u32([7]),
        Adbc.Column.u64([8]),
        Adbc.Column.u64([9]),
        Adbc.Column.string(["hello"]),
        Adbc.Column.string(["world"]),
        Adbc.Column.string(["goodbye"]),
        Adbc.Column.string(["world"]),
        Adbc.Column.string(["foo"]),
        Adbc.Column.string(["bar"]),
        Adbc.Column.binary([<<"data", 0x01, 0x02>>]),
        Adbc.Column.f32([1.1]),
        Adbc.Column.f64([2.2]),
        Adbc.Column.f32([3.3]),
        Adbc.Column.f64([4.4]),
        1.1,
        2.2,
        Adbc.Column.boolean([true]),
        "2021-01-01",
        "2021-01-01 00:00:00"
      ]
    )

    assert {:ok, results = %Adbc.Result{num_rows: nil, data: _}} =
             Connection.query(conn, "SELECT * FROM test")

    assert %Adbc.Result{
             num_rows: nil,
             data: [
               %Adbc.Column{name: "i1", type: :s64, nullable: true, metadata: nil, data: [1]},
               %Adbc.Column{name: "i2", type: :s64, nullable: true, metadata: nil, data: [2]},
               %Adbc.Column{name: "i3", type: :s64, nullable: true, metadata: nil, data: [3]},
               %Adbc.Column{name: "i4", type: :s64, nullable: true, metadata: nil, data: [4]},
               %Adbc.Column{name: "i5", type: :s64, nullable: true, metadata: nil, data: [5]},
               %Adbc.Column{name: "i6", type: :s64, nullable: true, metadata: nil, data: [6]},
               %Adbc.Column{name: "i7", type: :s64, nullable: true, metadata: nil, data: ~c"\a"},
               %Adbc.Column{name: "i8", type: :s64, nullable: true, metadata: nil, data: ~c"\b"},
               %Adbc.Column{name: "i9", type: :s64, nullable: true, metadata: nil, data: ~c"\t"},
               %Adbc.Column{
                 name: "t1",
                 type: :string,
                 nullable: true,
                 metadata: nil,
                 data: ["hello"]
               },
               %Adbc.Column{
                 name: "t2",
                 type: :string,
                 nullable: true,
                 metadata: nil,
                 data: ["world"]
               },
               %Adbc.Column{
                 name: "t3",
                 type: :string,
                 nullable: true,
                 metadata: nil,
                 data: ["goodbye"]
               },
               %Adbc.Column{
                 name: "t4",
                 type: :string,
                 nullable: true,
                 metadata: nil,
                 data: ["world"]
               },
               %Adbc.Column{
                 name: "t5",
                 type: :string,
                 nullable: true,
                 metadata: nil,
                 data: ["foo"]
               },
               %Adbc.Column{
                 name: "t6",
                 type: :string,
                 nullable: true,
                 metadata: nil,
                 data: ["bar"]
               },
               %Adbc.Column{
                 name: "b1",
                 type: :binary,
                 nullable: true,
                 metadata: nil,
                 data: [<<100, 97, 116, 97, 1, 2>>]
               },
               %Adbc.Column{
                 name: "r1",
                 type: :f64,
                 nullable: true,
                 metadata: nil,
                 data: [r1]
               },
               %Adbc.Column{name: "r2", type: :f64, nullable: true, metadata: nil, data: [2.2]},
               %Adbc.Column{
                 name: "r3",
                 type: :f64,
                 nullable: true,
                 metadata: nil,
                 data: [r3]
               },
               %Adbc.Column{name: "r4", type: :f64, nullable: true, metadata: nil, data: [4.4]},
               %Adbc.Column{name: "n1", type: :f64, nullable: true, metadata: nil, data: [1.1]},
               %Adbc.Column{name: "n2", type: :f64, nullable: true, metadata: nil, data: [2.2]},
               %Adbc.Column{name: "n3", type: :s64, nullable: true, metadata: nil, data: [1]},
               %Adbc.Column{
                 name: "n4",
                 type: :string,
                 nullable: true,
                 metadata: nil,
                 data: ["2021-01-01"]
               },
               %Adbc.Column{
                 name: "n5",
                 type: :string,
                 nullable: true,
                 metadata: nil,
                 data: ["2021-01-01 00:00:00"]
               }
             ]
           } = Adbc.Result.materialize(results)

    assert is_float(r1) and is_float(r3)
    assert abs(r1 - 1.1) < 1.0e-6
    assert abs(r3 - 3.3) < 1.0e-6
  end

  test "bulk-queries", %{db: _, conn: conn} do
    assert {:ok,
            results = %Adbc.Result{
              data: [
                %Adbc.Column{
                  name: "S64",
                  type: :s64,
                  metadata: nil,
                  nullable: true
                },
                %Adbc.Column{
                  name: "F64",
                  type: :f64,
                  metadata: nil,
                  nullable: true
                }
              ]
            }} =
             Connection.query(conn, "SELECT ? AS S64, ? AS F64", [
               Adbc.Column.s64([1, 2]),
               Adbc.Column.f64([3.3, 4.4])
             ])

    assert %Adbc.Result{
             data: [
               %Adbc.Column{
                 data: [1, 2],
                 metadata: nil,
                 name: "S64",
                 nullable: true,
                 type: :s64
               },
               %Adbc.Column{
                 name: "F64",
                 type: :f64,
                 nullable: true,
                 metadata: nil,
                 data: [3.3, 4.4]
               }
             ],
             num_rows: nil
           } = Adbc.Result.materialize(results)
  end
end
