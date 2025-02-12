defmodule Adbc.IPC.Test do
  use ExUnit.Case

  alias Adbc.IPC

  @invalid_data File.read!(Path.join(__DIR__, "invalid.arrows"))
  @valid_schema_only File.read!(Path.join(__DIR__, "schema-valid.arrows"))

  describe "load" do
    test "it can load ipc from in-memory data" do
      f32_values = [1.1, 2.2, 3.3]

      assert {:ok, data} =
               IPC.dump_ipc([Adbc.Column.s64([1, 2, 3]), Adbc.Column.f32(f32_values)])

      assert {:ok,
              results = %Adbc.Result{
                data: [
                  %Adbc.Column{
                    name: "",
                    type: :s64,
                    metadata: nil,
                    nullable: true
                  },
                  %Adbc.Column{
                    name: "",
                    type: :f32,
                    metadata: nil,
                    nullable: true
                  }
                ],
                num_rows: nil
              }} = IPC.load_ipc(data)

      assert %Adbc.Result{
               data: [
                 %Adbc.Column{
                   name: "",
                   type: :s64,
                   nullable: true,
                   metadata: nil,
                   data: [1, 2, 3]
                 },
                 %Adbc.Column{
                   name: "",
                   type: :f32,
                   nullable: true,
                   metadata: nil,
                   data: f32
                 }
               ]
             } = Adbc.Result.materialize(results)

      assert Enum.all?(Enum.zip(f32_values, f32), fn {a, b} -> abs(a - b) < 0.001 end)
    end

    test "it returns empty Adbc.Result when im-memory IPC contains only schema" do
      assert {:ok, %Adbc.Result{data: [], num_rows: nil}} = IPC.load_ipc(@valid_schema_only)
    end

    test "it returns Adbc.Error when loading invalid in-memory ipc data" do
      assert {:error, error} = IPC.load_ipc(@invalid_data)

      assert Exception.message(error) ==
               "Expected 0xFFFFFFFF at start of message but found 0xFFFFFF00"
    end
  end

  describe "dump" do
    test "it can dump columns as in-memory IPC format data" do
      assert {:ok, _data} =
               IPC.dump_ipc([Adbc.Column.s64([1, 2, 3]), Adbc.Column.f32([1.1, 2.2, 3.3])])
    end
  end
end
