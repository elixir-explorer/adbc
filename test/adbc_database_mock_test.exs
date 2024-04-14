defmodule Adbc.Mocks.Database do
  def string_option_1, do: :"adbc.mocks.database.string_option_1"
  def string_option_2, do: :"adbc.mocks.database.string_option_2"
  def string_option_3, do: :"adbc.mocks.database.string_option_3"
  def binary_option_1, do: :"adbc.mocks.database.bytes_option_1"
  def binary_option_2, do: :"adbc.mocks.database.bytes_option_2"
  def binary_option_3, do: :"adbc.mocks.database.bytes_option_3"
  def integer_option_1, do: :"adbc.mocks.database.int_option_1"
  def integer_option_2, do: :"adbc.mocks.database.int_option_2"
  def integer_option_3, do: :"adbc.mocks.database.int_option_3"
  def float_option_1, do: :"adbc.mocks.database.double_option_1"
  def float_option_2, do: :"adbc.mocks.database.double_option_2"
  def float_option_3, do: :"adbc.mocks.database.double_option_3"

  def set_option(opts \\ [], key, value) when is_list(opts) do
    Keyword.put(opts, key, value)
  end
end

defmodule Adbc.Database.Mock.Test do
  use ExUnit.Case

  alias Adbc.Database
  alias Adbc.Mocks.Database, as: MD

  setup do
    startup_options =
      MD.set_option(:startup, "passed in start_supervised!")
      |> MD.set_option(:driver, System.get_env("ADBC_DRIVER_MOCKS"))

    db = start_supervised!({Adbc.Database, startup_options})
    %{db: db}
  end

  describe "get/set database options" do
    @describetag :mocks

    test "get options passed in start_supervised!", %{db: db} do
      assert {:ok, "passed in start_supervised!"} == Database.get_string_option(db, :startup)
    end

    test "override options passed in start_supervised!", %{db: db} do
      assert :ok == Database.set_option(db, :startup, "overridden in test")
      assert {:ok, "overridden in test"} == Database.get_string_option(db, :startup)
    end

    test "set string option 1", %{db: db} do
      assert :ok == Database.set_option(db, MD.string_option_1(), "string option 1")
      assert {:ok, "string option 1"} == Database.get_string_option(db, MD.string_option_1())
      assert :ok == Database.set_option(db, MD.string_option_1(), "string option 1 new value")

      assert {:ok, "string option 1 new value"} ==
               Database.get_string_option(db, MD.string_option_1())
    end

    test "set multiple string options", %{db: db} do
      assert :ok == Database.set_option(db, MD.string_option_1(), "string option 1")
      assert :ok == Database.set_option(db, MD.string_option_2(), "string option 2")
      assert :ok == Database.set_option(db, MD.string_option_3(), "string option 3")
      assert {:ok, "string option 1"} == Database.get_string_option(db, MD.string_option_1())
      assert {:ok, "string option 2"} == Database.get_string_option(db, MD.string_option_2())
      assert {:ok, "string option 3"} == Database.get_string_option(db, MD.string_option_3())

      assert :ok == Database.set_option(db, MD.string_option_1(), "string option 1 new value")
      assert :ok == Database.set_option(db, MD.string_option_2(), "string option 2 new value")
      assert :ok == Database.set_option(db, MD.string_option_3(), "string option 3 new value")

      assert {:ok, "string option 1 new value"} ==
               Database.get_string_option(db, MD.string_option_1())

      assert {:ok, "string option 2 new value"} ==
               Database.get_string_option(db, MD.string_option_2())

      assert {:ok, "string option 3 new value"} ==
               Database.get_string_option(db, MD.string_option_3())
    end

    test "set multiple binary options", %{db: db} do
      assert :ok == Database.set_option(db, MD.binary_option_1(), {:binary, "binary option 1"})
      assert :ok == Database.set_option(db, MD.binary_option_2(), {:binary, "binary option 2"})
      assert :ok == Database.set_option(db, MD.binary_option_3(), {:binary, "binary option 3"})
      assert {:ok, "binary option 1"} == Database.get_binary_option(db, MD.binary_option_1())
      assert {:ok, "binary option 2"} == Database.get_binary_option(db, MD.binary_option_2())
      assert {:ok, "binary option 3"} == Database.get_binary_option(db, MD.binary_option_3())

      assert :ok ==
               Database.set_option(
                 db,
                 MD.binary_option_1(),
                 {:binary, "binary option 1 new value"}
               )

      assert :ok ==
               Database.set_option(
                 db,
                 MD.binary_option_2(),
                 {:binary, "binary option 2 new value"}
               )

      assert :ok ==
               Database.set_option(
                 db,
                 MD.binary_option_3(),
                 {:binary, "binary option 3 new value"}
               )

      assert {:ok, "binary option 1 new value"} ==
               Database.get_binary_option(db, MD.binary_option_1())

      assert {:ok, "binary option 2 new value"} ==
               Database.get_binary_option(db, MD.binary_option_2())

      assert {:ok, "binary option 3 new value"} ==
               Database.get_binary_option(db, MD.binary_option_3())
    end

    test "set multiple integer options", %{db: db} do
      assert :ok == Database.set_option(db, MD.integer_option_1(), 1)
      assert :ok == Database.set_option(db, MD.integer_option_2(), 2)
      assert :ok == Database.set_option(db, MD.integer_option_3(), 3)
      assert {:ok, 1} == Database.get_integer_option(db, MD.integer_option_1())
      assert {:ok, 2} == Database.get_integer_option(db, MD.integer_option_2())
      assert {:ok, 3} == Database.get_integer_option(db, MD.integer_option_3())

      assert :ok == Database.set_option(db, MD.integer_option_1(), 10)
      assert :ok == Database.set_option(db, MD.integer_option_2(), 20)
      assert :ok == Database.set_option(db, MD.integer_option_3(), 30)
      assert {:ok, 10} == Database.get_integer_option(db, MD.integer_option_1())
      assert {:ok, 20} == Database.get_integer_option(db, MD.integer_option_2())
      assert {:ok, 30} == Database.get_integer_option(db, MD.integer_option_3())
    end

    test "set multiple float options", %{db: db} do
      assert :ok == Database.set_option(db, MD.float_option_1(), 100.0)
      assert :ok == Database.set_option(db, MD.float_option_2(), 200.0)
      assert :ok == Database.set_option(db, MD.float_option_3(), 300.0)
      assert {:ok, 100.0} == Database.get_float_option(db, MD.float_option_1())
      assert {:ok, 200.0} == Database.get_float_option(db, MD.float_option_2())
      assert {:ok, 300.0} == Database.get_float_option(db, MD.float_option_3())

      assert :ok == Database.set_option(db, MD.float_option_1(), 10000.0)
      assert :ok == Database.set_option(db, MD.float_option_2(), 20000.0)
      assert :ok == Database.set_option(db, MD.float_option_3(), 30000.0)
      assert {:ok, 10000.0} == Database.get_float_option(db, MD.float_option_1())
      assert {:ok, 20000.0} == Database.get_float_option(db, MD.float_option_2())
      assert {:ok, 30000.0} == Database.get_float_option(db, MD.float_option_3())
    end
  end

  describe "get/set database options, expected error" do
    @describetag :mocks

    test "get non-existed option", %{db: db} do
      assert {:error, %Adbc.Error{} = error} = Database.get_string_option(db, "expected-error")

      assert Exception.message(error) ==
               "[Mocks] unknown database string type option `expected-error`"

      assert {:error, %Adbc.Error{} = error} = Database.get_binary_option(db, "expected-error")

      assert Exception.message(error) ==
               "[Mocks] unknown database bytes type option `expected-error`"

      assert {:error, %Adbc.Error{} = error} = Database.get_integer_option(db, "expected-error")

      assert Exception.message(error) ==
               "[Mocks] unknown database int type option `expected-error`"

      assert {:error, %Adbc.Error{} = error} = Database.get_float_option(db, "expected-error")

      assert Exception.message(error) ==
               "[Mocks] unknown database double type option `expected-error`"
    end

    test "set non-existed option", %{db: db} do
      assert {:error, %Adbc.Error{} = error} = Database.set_option(db, "expected-error", "bar")

      assert Exception.message(error) ==
               "[Mocks] unknown database string type option `expected-error`"

      assert {:error, %Adbc.Error{} = error} =
               Database.set_option(db, "expected-error", {:binary, "bar"})

      assert Exception.message(error) ==
               "[Mocks] unknown database bytes type option `expected-error`"

      assert {:error, %Adbc.Error{} = error} = Database.set_option(db, "expected-error", 1)

      assert Exception.message(error) ==
               "[Mocks] unknown database int type option `expected-error`"

      assert {:error, %Adbc.Error{} = error} = Database.set_option(db, "expected-error", 1.0)

      assert Exception.message(error) ==
               "[Mocks] unknown database double type option `expected-error`"
    end
  end
end
