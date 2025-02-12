defmodule Adbc.IPC.Test do
  use ExUnit.Case

  alias Adbc.IPC

  @invalid_data File.read!(Path.join(__DIR__, "invalid.arrows"))
  @valid_schema_only File.read!(Path.join(__DIR__, "schema-valid.arrows"))
  @simple_data File.read!(Path.join(__DIR__, "simple.arrows"))

  describe "load" do
    test "it can load ipc from in-memory data" do
      assert :ok = IPC.load_ipc(@simple_data)
    end

    test "it returns Adbc.Error when in-memory ipc data contains only schema" do
      assert {:error, error} = IPC.load_ipc(@valid_schema_only)
      assert Exception.message(error) == "decoder did not just decode a RecordBatch message"
    end

    test "it returns Adbc.Error when loading invalid in-memory ipc data" do
      assert {:error, error} = IPC.load_ipc(@invalid_data)

      assert Exception.message(error) ==
               "Expected 0xFFFFFFFF at start of message but found 0xFFFFFF00"
    end
  end

  describe "dump" do
    test "it can dump columns as in-memory IPC format data" do
      assert {:ok, data} = IPC.dump_ipc([Adbc.Column.s64([1, 2, 3]), Adbc.Column.f32([1.1, 2.2, 3.3])])
      File.write!(Path.join(__DIR__, "simple.arrows"), data)
    end
  end
end
