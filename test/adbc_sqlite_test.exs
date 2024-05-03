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

    {:ok,
     %Adbc.Result{
       num_rows: nil,
       data: %{
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
       }
     }} =
      Adbc.Connection.query(conn, "SELECT * FROM test")
  end

  test "insert with Adbc.Buffer", %{db: _, conn: conn} do
    Connection.query(
      conn,
      """
      INSERT INTO test (i1, i2, i3, i4, i5, i6, i7, i8, i9, t1, t2, t3, t4, t5, t6, b1, r1, r2, r3, r4, n1, n2, n3, n4, n5)
      VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      """,
      [
        Adbc.Buffer.i8([1]),
        Adbc.Buffer.i16([2]),
        Adbc.Buffer.i32([3]),
        Adbc.Buffer.i64([4]),
        Adbc.Buffer.u8([5]),
        Adbc.Buffer.u16([6]),
        Adbc.Buffer.u32([7]),
        Adbc.Buffer.u64([8]),
        Adbc.Buffer.u64([9]),
        Adbc.Buffer.string(["hello"]),
        Adbc.Buffer.string(["world"]),
        Adbc.Buffer.string(["goodbye"]),
        Adbc.Buffer.string(["world"]),
        Adbc.Buffer.string(["foo"]),
        Adbc.Buffer.string(["bar"]),
        Adbc.Buffer.binary([<<"data", 0x01, 0x02>>]),
        Adbc.Buffer.f32([1.1]),
        Adbc.Buffer.f64([2.2]),
        Adbc.Buffer.f32([3.3]),
        Adbc.Buffer.f64([4.4]),
        1.1,
        2.2,
        Adbc.Buffer.boolean([true]),
        "2021-01-01",
        "2021-01-01 00:00:00"
      ]
    )

    {:ok,
     %Adbc.Result{
       num_rows: nil,
       data: %{
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
         "r1" => [r1],
         "r2" => [2.2],
         "r3" => [r3],
         "r4" => [4.4],
         "t1" => ["hello"],
         "t2" => ["world"],
         "t3" => ["goodbye"],
         "t4" => ["world"],
         "t5" => ["foo"],
         "t6" => ["bar"]
       }
     }} = Connection.query(conn, "SELECT * FROM test")

    assert is_float(r1) and is_float(r3)
    assert abs(r1 - 1.1) < 1.0e-6
    assert abs(r3 - 3.3) < 1.0e-6
  end
end
