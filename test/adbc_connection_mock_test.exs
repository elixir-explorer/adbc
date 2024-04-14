defmodule Adbc.Mocks.Connection do
  def string_option_1, do: :"adbc.mocks.connection.string_option_1"
  def string_option_2, do: :"adbc.mocks.connection.string_option_2"
  def string_option_3, do: :"adbc.mocks.connection.string_option_3"
  def binary_option_1, do: :"adbc.mocks.connection.bytes_option_1"
  def binary_option_2, do: :"adbc.mocks.connection.bytes_option_2"
  def binary_option_3, do: :"adbc.mocks.connection.bytes_option_3"
  def integer_option_1, do: :"adbc.mocks.connection.int_option_1"
  def integer_option_2, do: :"adbc.mocks.connection.int_option_2"
  def integer_option_3, do: :"adbc.mocks.connection.int_option_3"
  def float_option_1, do: :"adbc.mocks.connection.double_option_1"
  def float_option_2, do: :"adbc.mocks.connection.double_option_2"
  def float_option_3, do: :"adbc.mocks.connection.double_option_3"

  def set_option(opts \\ [], key, value) when is_list(opts) do
    Keyword.put(opts, key, value)
  end
end

defmodule Adbc.Connection.Mock.Test do
  use ExUnit.Case

  alias Adbc.Connection
  alias Adbc.Mocks.Connection, as: MC

  setup do
    db =
      start_supervised!({Adbc.Database, driver: System.get_env("ADBC_DRIVER_MOCKS")})

    startup_options =
      MC.set_option(:startup, "passed in start_supervised!")
      |> MC.set_option(:database, db)

    {:ok, conn} = Connection.start_link(startup_options)
    %{db: db, conn: conn}
  end

  describe "get/set connection options" do
    @describetag :mocks

    test "get options passed in start_supervised!", %{db: _db, conn: conn} do
      assert {:ok, "passed in start_supervised!"} == Connection.get_string_option(conn, :startup)
    end

    test "override options passed in start_supervised!", %{db: _db, conn: conn} do
      assert :ok == Connection.set_option(conn, :startup, "overridden in test")
      assert {:ok, "overridden in test"} == Connection.get_string_option(conn, :startup)
    end

    test "set string option 1", %{db: _db, conn: conn} do
      assert :ok == Connection.set_option(conn, MC.string_option_1(), "string option 1")
      assert {:ok, "string option 1"} == Connection.get_string_option(conn, MC.string_option_1())
      assert :ok == Connection.set_option(conn, MC.string_option_1(), "string option 1 new value")

      assert {:ok, "string option 1 new value"} ==
               Connection.get_string_option(conn, MC.string_option_1())
    end

    test "set multiple string options", %{db: _db, conn: conn} do
      assert :ok == Connection.set_option(conn, MC.string_option_1(), "string option 1")
      assert :ok == Connection.set_option(conn, MC.string_option_2(), "string option 2")
      assert :ok == Connection.set_option(conn, MC.string_option_3(), "string option 3")
      assert {:ok, "string option 1"} == Connection.get_string_option(conn, MC.string_option_1())
      assert {:ok, "string option 2"} == Connection.get_string_option(conn, MC.string_option_2())
      assert {:ok, "string option 3"} == Connection.get_string_option(conn, MC.string_option_3())

      assert :ok == Connection.set_option(conn, MC.string_option_1(), "string option 1 new value")
      assert :ok == Connection.set_option(conn, MC.string_option_2(), "string option 2 new value")
      assert :ok == Connection.set_option(conn, MC.string_option_3(), "string option 3 new value")

      assert {:ok, "string option 1 new value"} ==
               Connection.get_string_option(conn, MC.string_option_1())

      assert {:ok, "string option 2 new value"} ==
               Connection.get_string_option(conn, MC.string_option_2())

      assert {:ok, "string option 3 new value"} ==
               Connection.get_string_option(conn, MC.string_option_3())
    end

    test "set multiple binary options", %{db: _db, conn: conn} do
      assert :ok ==
               Connection.set_option(conn, MC.binary_option_1(), {:binary, "binary option 1"})

      assert :ok ==
               Connection.set_option(conn, MC.binary_option_2(), {:binary, "binary option 2"})

      assert :ok ==
               Connection.set_option(conn, MC.binary_option_3(), {:binary, "binary option 3"})

      assert {:ok, "binary option 1"} == Connection.get_binary_option(conn, MC.binary_option_1())
      assert {:ok, "binary option 2"} == Connection.get_binary_option(conn, MC.binary_option_2())
      assert {:ok, "binary option 3"} == Connection.get_binary_option(conn, MC.binary_option_3())

      assert :ok ==
               Connection.set_option(
                 conn,
                 MC.binary_option_1(),
                 {:binary, "binary option 1 new value"}
               )

      assert :ok ==
               Connection.set_option(
                 conn,
                 MC.binary_option_2(),
                 {:binary, "binary option 2 new value"}
               )

      assert :ok ==
               Connection.set_option(
                 conn,
                 MC.binary_option_3(),
                 {:binary, "binary option 3 new value"}
               )

      assert {:ok, "binary option 1 new value"} ==
               Connection.get_binary_option(conn, MC.binary_option_1())

      assert {:ok, "binary option 2 new value"} ==
               Connection.get_binary_option(conn, MC.binary_option_2())

      assert {:ok, "binary option 3 new value"} ==
               Connection.get_binary_option(conn, MC.binary_option_3())
    end

    test "set multiple integer options", %{db: _db, conn: conn} do
      assert :ok == Connection.set_option(conn, MC.integer_option_1(), 1)
      assert :ok == Connection.set_option(conn, MC.integer_option_2(), 2)
      assert :ok == Connection.set_option(conn, MC.integer_option_3(), 3)
      assert {:ok, 1} == Connection.get_integer_option(conn, MC.integer_option_1())
      assert {:ok, 2} == Connection.get_integer_option(conn, MC.integer_option_2())
      assert {:ok, 3} == Connection.get_integer_option(conn, MC.integer_option_3())

      assert :ok == Connection.set_option(conn, MC.integer_option_1(), 10)
      assert :ok == Connection.set_option(conn, MC.integer_option_2(), 20)
      assert :ok == Connection.set_option(conn, MC.integer_option_3(), 30)
      assert {:ok, 10} == Connection.get_integer_option(conn, MC.integer_option_1())
      assert {:ok, 20} == Connection.get_integer_option(conn, MC.integer_option_2())
      assert {:ok, 30} == Connection.get_integer_option(conn, MC.integer_option_3())
    end

    test "set multiple float options", %{db: _db, conn: conn} do
      assert :ok == Connection.set_option(conn, MC.float_option_1(), 100.0)
      assert :ok == Connection.set_option(conn, MC.float_option_2(), 200.0)
      assert :ok == Connection.set_option(conn, MC.float_option_3(), 300.0)
      assert {:ok, 100.0} == Connection.get_float_option(conn, MC.float_option_1())
      assert {:ok, 200.0} == Connection.get_float_option(conn, MC.float_option_2())
      assert {:ok, 300.0} == Connection.get_float_option(conn, MC.float_option_3())

      assert :ok == Connection.set_option(conn, MC.float_option_1(), 10000.0)
      assert :ok == Connection.set_option(conn, MC.float_option_2(), 20000.0)
      assert :ok == Connection.set_option(conn, MC.float_option_3(), 30000.0)
      assert {:ok, 10000.0} == Connection.get_float_option(conn, MC.float_option_1())
      assert {:ok, 20000.0} == Connection.get_float_option(conn, MC.float_option_2())
      assert {:ok, 30000.0} == Connection.get_float_option(conn, MC.float_option_3())
    end
  end

  describe "get/set connection options, expected error" do
    @describetag :mocks

    test "get non-existed option", %{db: _db, conn: conn} do
      assert {:error, %Adbc.Error{} = error} =
               Connection.get_string_option(conn, "expected-error")

      assert Exception.message(error) ==
               "[Mocks] unknown connection string type option `expected-error`"

      assert {:error, %Adbc.Error{} = error} =
               Connection.get_binary_option(conn, "expected-error")

      assert Exception.message(error) ==
               "[Mocks] unknown connection bytes type option `expected-error`"

      assert {:error, %Adbc.Error{} = error} =
               Connection.get_integer_option(conn, "expected-error")

      assert Exception.message(error) ==
               "[Mocks] unknown connection int type option `expected-error`"

      assert {:error, %Adbc.Error{} = error} = Connection.get_float_option(conn, "expected-error")

      assert Exception.message(error) ==
               "[Mocks] unknown connection double type option `expected-error`"
    end

    test "set non-existed option", %{db: _db, conn: conn} do
      assert {:error, %Adbc.Error{} = error} =
               Connection.set_option(conn, "expected-error", "bar")

      assert Exception.message(error) ==
               "[Mocks] unknown connection string type option `expected-error`"

      assert {:error, %Adbc.Error{} = error} =
               Connection.set_option(conn, "expected-error", {:binary, "bar"})

      assert Exception.message(error) ==
               "[Mocks] unknown connection bytes type option `expected-error`"

      assert {:error, %Adbc.Error{} = error} = Connection.set_option(conn, "expected-error", 1)

      assert Exception.message(error) ==
               "[Mocks] unknown connection int type option `expected-error`"

      assert {:error, %Adbc.Error{} = error} = Connection.set_option(conn, "expected-error", 1.0)

      assert Exception.message(error) ==
               "[Mocks] unknown connection double type option `expected-error`"
    end
  end
end
