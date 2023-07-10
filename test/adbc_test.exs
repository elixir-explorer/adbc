defmodule AdbcTest do
  use ExUnit.Case, async: true
  doctest Adbc

  alias Adbc.{Database, Connection}

  describe "download_driver" do
    test "multiple times" do
      assert Adbc.download_driver!(:sqlite) == :ok
      assert Adbc.download_driver!(:sqlite) == :ok
    end
  end

  describe "postgresql smoke tests" do
    @describetag :postgresql

    test "runs queries" do
      db =
        start_supervised!({Database, driver: :postgresql, uri: "postgres://postgres@localhost"})

      conn = start_supervised!({Connection, database: db})

      assert {:ok, %Adbc.Result{data: %{"num" => [123]}}} =
               Connection.query(conn, "SELECT 123 as num")
    end
  end
end
