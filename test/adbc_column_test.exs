defmodule Adbc.Column.Test do
  use ExUnit.Case
  doctest Adbc.Column

  describe "decimals" do
    test "integers" do
      value = 42
      bitwidth = 128
      precision = 19
      scale = 10
      decimal = Decimal.new(value)

      assert %Adbc.Column{
               data: [decimal_data, value_data],
               metadata: nil,
               name: nil,
               nullable: false,
               type: {:decimal, ^bitwidth, ^precision, ^scale}
             } = Adbc.Column.decimal128([decimal, value], precision, scale)

      assert <<decode1::signed-integer-little-size(bitwidth)>> = decimal_data
      assert value == decode1 / :math.pow(10, scale)

      assert <<decode2::signed-integer-little-size(bitwidth)>> = value_data
      assert value == decode2 / :math.pow(10, scale)

      bitwidth = 256

      assert %Adbc.Column{
               data: [decimal_data, value_data],
               metadata: nil,
               name: nil,
               nullable: false,
               type: {:decimal, ^bitwidth, ^precision, ^scale}
             } = Adbc.Column.decimal256([decimal, value], precision, scale)

      assert <<decode1::signed-integer-little-size(bitwidth)>> = decimal_data
      assert value == decode1 / :math.pow(10, scale)

      assert <<decode2::signed-integer-little-size(bitwidth)>> = value_data
      assert value == decode2 / :math.pow(10, scale)
    end

    test "nil in data" do
      bitwidth = 128
      precision = 19
      scale = 10

      decimal_with_nil = %Adbc.Column{
        data: [nil],
        metadata: nil,
        name: nil,
        nullable: true,
        type: {:decimal, bitwidth, precision, scale}
      }

      assert %Adbc.Column{
               name: nil,
               type: {:decimal, ^bitwidth, ^precision, ^scale},
               nullable: true,
               metadata: nil,
               data: [nil],
               length: nil,
               offset: nil
             } = Adbc.Column.materialize(decimal_with_nil)
    end

    test "floats" do
      value = 12345

      bitwidth = 128
      precision = 5
      scale = 10

      exp = -3
      actual_value = value * :math.pow(10, exp)
      decimal = Decimal.new(1, value, exp)

      assert %Adbc.Column{
               data: [data],
               metadata: nil,
               name: nil,
               nullable: false,
               type: {:decimal, ^bitwidth, ^precision, ^scale}
             } = Adbc.Column.decimal128([decimal], precision, scale)

      assert <<decode::signed-integer-little-size(bitwidth)>> = data
      assert actual_value == decode / :math.pow(10, scale)

      bitwidth = 256

      assert %Adbc.Column{
               data: [data],
               metadata: nil,
               name: nil,
               nullable: false,
               type: {:decimal, ^bitwidth, ^precision, ^scale}
             } = Adbc.Column.decimal256([decimal], precision, scale)

      assert <<decode::signed-integer-little-size(bitwidth)>> = data
      assert actual_value == decode / :math.pow(10, scale)
    end

    test "raise if precision value is insufficient" do
      value = 54321
      bitwidth = 128
      precision = 4
      scale = 1
      decimal = Decimal.new(value)

      assert_raise Adbc.Error,
                   "`54321` cannot be fitted into a decimal128 with the specified precision 4",
                   fn ->
                     Adbc.Column.decimal128([decimal], precision, scale)
                   end

      assert_raise Adbc.Error,
                   "`54321` cannot be fitted into a decimal128 with the specified precision 4",
                   fn ->
                     Adbc.Column.decimal128([value], precision, scale)
                   end

      assert_raise Adbc.Error,
                   "`54321` cannot be fitted into a decimal256 with the specified precision 4",
                   fn ->
                     Adbc.Column.decimal256([decimal], precision, scale)
                   end

      assert_raise Adbc.Error,
                   "`54321` cannot be fitted into a decimal256 with the specified precision 4",
                   fn ->
                     Adbc.Column.decimal256([value], precision, scale)
                   end

      precision = 5

      assert %Adbc.Column{
               data: [decimal_data, value_data],
               metadata: nil,
               name: nil,
               nullable: false,
               type: {:decimal, ^bitwidth, ^precision, ^scale}
             } = Adbc.Column.decimal128([decimal, value], precision, scale)

      assert <<decode1::signed-integer-little-size(bitwidth)>> = decimal_data
      assert value == decode1 / :math.pow(10, scale)

      assert <<decode2::signed-integer-little-size(bitwidth)>> = value_data
      assert value == decode2 / :math.pow(10, scale)

      bitwidth = 256

      assert %Adbc.Column{
               data: [decimal_data, value_data],
               metadata: nil,
               name: nil,
               nullable: false,
               type: {:decimal, ^bitwidth, ^precision, ^scale}
             } = Adbc.Column.decimal256([decimal, value], precision, scale)

      assert <<decode1::signed-integer-little-size(bitwidth)>> = decimal_data
      assert value == decode1 / :math.pow(10, scale)

      assert <<decode2::signed-integer-little-size(bitwidth)>> = value_data
      assert value == decode2 / :math.pow(10, scale)
    end

    test "raise if scale value is insufficient" do
      value = 54321
      bitwidth = 128
      precision = 5
      scale = 1

      exp = -2
      actual_value = value * :math.pow(10, exp)
      decimal = Decimal.new(1, value, exp)

      assert_raise Adbc.Error,
                   "`543.21` with exponent `-2` cannot be represented as a valid decimal128 number with scale value `1`",
                   fn ->
                     Adbc.Column.decimal128([decimal], precision, scale)
                   end

      assert_raise Adbc.Error,
                   "`543.21` with exponent `-2` cannot be represented as a valid decimal256 number with scale value `1`",
                   fn ->
                     Adbc.Column.decimal256([decimal], precision, scale)
                   end

      scale = 2

      assert %Adbc.Column{
               data: [data],
               metadata: nil,
               name: nil,
               nullable: false,
               type: {:decimal, ^bitwidth, ^precision, ^scale}
             } = Adbc.Column.decimal128([decimal], precision, scale)

      assert <<decode::signed-integer-little-size(bitwidth)>> = data
      assert actual_value == decode / :math.pow(10, scale)

      bitwidth = 256

      assert %Adbc.Column{
               data: [data],
               metadata: nil,
               name: nil,
               nullable: false,
               type: {:decimal, ^bitwidth, ^precision, ^scale}
             } = Adbc.Column.decimal256([decimal], precision, scale)

      assert <<decode::signed-integer-little-size(bitwidth)>> = data
      assert actual_value == decode / :math.pow(10, scale)
    end

    test "raise on Inf, -Inf and NaN" do
      assert_raise Adbc.Error,
                   "`Infinity` cannot be represented as a valid decimal128 number",
                   fn ->
                     Adbc.Column.decimal128([Decimal.new("inf")], 19, 10)
                   end

      assert_raise Adbc.Error,
                   "`Infinity` cannot be represented as a valid decimal128 number",
                   fn ->
                     Adbc.Column.decimal128([Decimal.new("1"), Decimal.new("inf")], 19, 10)
                   end

      assert_raise Adbc.Error,
                   "`-Infinity` cannot be represented as a valid decimal128 number",
                   fn ->
                     Adbc.Column.decimal128([Decimal.new("-inf")], 19, 10)
                   end

      assert_raise Adbc.Error,
                   "`-Infinity` cannot be represented as a valid decimal128 number",
                   fn ->
                     Adbc.Column.decimal128([Decimal.new("1"), Decimal.new("-inf")], 19, 10)
                   end

      assert_raise Adbc.Error, "`NaN` cannot be represented as a valid decimal128 number", fn ->
        Adbc.Column.decimal128([Decimal.new("NaN")], 19, 10)
      end

      assert_raise Adbc.Error,
                   "`Infinity` cannot be represented as a valid decimal256 number",
                   fn ->
                     Adbc.Column.decimal256([Decimal.new("inf")], 19, 10)
                   end

      assert_raise Adbc.Error,
                   "`Infinity` cannot be represented as a valid decimal256 number",
                   fn ->
                     Adbc.Column.decimal256([Decimal.new("1"), Decimal.new("inf")], 19, 10)
                   end

      assert_raise Adbc.Error,
                   "`-Infinity` cannot be represented as a valid decimal256 number",
                   fn ->
                     Adbc.Column.decimal256([Decimal.new("-inf")], 19, 10)
                   end

      assert_raise Adbc.Error,
                   "`-Infinity` cannot be represented as a valid decimal256 number",
                   fn ->
                     Adbc.Column.decimal256([Decimal.new("1"), Decimal.new("-inf")], 19, 10)
                   end

      assert_raise Adbc.Error, "`NaN` cannot be represented as a valid decimal256 number", fn ->
        Adbc.Column.decimal256([Decimal.new("NaN")], 19, 10)
      end
    end
  end

  describe "list view" do
    test "nested list view to list" do
      list_view = %Adbc.Column{
        name: "my_array",
        type: :list_view,
        nullable: true,
        metadata: nil,
        data: %{
          values: %Adbc.Column{
            name: "item",
            type: :s32,
            nullable: false,
            metadata: nil,
            data: [0, -127, 127, 50, 12, -7, 25]
          },
          validity: [true, false, true, true, true],
          offsets: [4, 7, 0, 0, 3],
          sizes: [3, 0, 4, 0, 2]
        }
      }

      assert Adbc.Column.to_list(list_view) == [
               [12, -7, 25],
               nil,
               [0, -127, 127, 50],
               [],
               ~c"2\f"
             ]

      nested_list_view = %Adbc.Column{
        name: nil,
        type: :list_view,
        nullable: true,
        metadata: nil,
        data: %{
          values: list_view,
          validity: [true, false, true, true, true],
          offsets: [2, 5, 0, 0, 3],
          sizes: [2, 0, 1, 0, 2]
        }
      }

      assert Adbc.Column.to_list(nested_list_view) == [
               [[0, -127, 127, 50], []],
               nil,
               [[12, -7, 25]],
               [],
               [[], ~c"2\f"]
             ]
    end
  end

  describe "run-end encoded array" do
    test "run-end encoded array to list" do
      # in this test case we construct a run-end encoded array
      # with logical length = 7 and offset = 0
      # and the values are float32s
      #
      # the virtual big array:
      #   type: :f32
      #   [1.0, 1.0, 1.0, 1.0, null, null, 2.0]
      #    ^                               ^
      #    |- offset = 0                   |- length = 7
      #
      # run-end encoded array:
      #   run_ends<:s32>: [4, 6, 7]
      #   values<:f32>: [1.0, null, 2.0]
      run_end_array = %Adbc.Column{
        name: "sample_run_end_encoded_array",
        type: :run_end_encoded,
        nullable: false,
        metadata: nil,
        length: 7,
        offset: 0,
        data: %{
          values: %Adbc.Column{
            name: "values",
            type: :f32,
            nullable: true,
            metadata: nil,
            data: [1.0, nil, 2.0]
          },
          run_ends: %Adbc.Column{
            name: "run_ends",
            type: :s32,
            nullable: false,
            metadata: nil,
            data: [4, 6, 7]
          }
        }
      }

      assert Adbc.Column.to_list(run_end_array) ==
               [1.0, 1.0, 1.0, 1.0, nil, nil, 2.0]

      # change logical length = 6 and offset = 1
      #  [1.0, 1.0, 1.0, 1.0, null, null, 2.0]
      #        ^                          ^
      #        |- offset = 1              |- length = 6
      assert Adbc.Column.to_list(%{run_end_array | offset: 1, length: 6}) ==
               [1.0, 1.0, 1.0, nil, nil, 2.0]

      # change logical length = 7 and offset = 1
      #  [1.0, 1.0, 1.0, 1.0, null, null, 2.0]
      #        ^                               ^
      #        |- offset = 1                   |- length = 7
      assert_raise Adbc.Error,
                   "Last run end is 7 but it should >= 8 (offset: 1, length: 7)",
                   fn ->
                     Adbc.Column.to_list(%{run_end_array | offset: 1, length: 7})
                   end

      # change logical length = 8 and offset = 0
      #  [1.0, 1.0, 1.0, 1.0, null, null, 2.0]
      #  ^                                     ^
      #  |- offset = 0                         |- length = 8
      assert_raise Adbc.Error,
                   "Last run end is 7 but it should >= 8 (offset: 0, length: 8)",
                   fn ->
                     Adbc.Column.to_list(%{run_end_array | offset: 0, length: 8})
                   end
    end

    test "nested run-end encoded arrays to list" do
      # in this test case we construct a nested run-end encoded array
      #
      # run-end encoded array: `[1, 2, 2]`
      #   <offset = 2, length = 3>
      #   run_ends<:s32>: [2, 4, 7]
      #   values: `[1, 1, 2]`
      #     {
      #         <offset = 2, length=3>
      #         run_ends<:s32>: [4, 6],
      #         values<:s32>: [1, 2]
      #     }
      inner_run_end_array = %Adbc.Column{
        name: "inner_run_end_encoded_array",
        type: :run_end_encoded,
        nullable: true,
        metadata: nil,
        length: 3,
        offset: 2,
        data: %{
          run_ends: %Adbc.Column{
            name: "run_ends",
            type: :s32,
            nullable: false,
            metadata: nil,
            data: [4, 6]
          },
          values: %Adbc.Column{
            name: "values",
            type: :s32,
            nullable: false,
            metadata: nil,
            data: [1, 2]
          }
        }
      }

      assert Adbc.Column.to_list(inner_run_end_array) == [1, 1, 2]

      run_end_array = %Adbc.Column{
        name: "sample_run_end_encoded_array",
        type: :run_end_encoded,
        nullable: false,
        metadata: nil,
        length: 3,
        offset: 3,
        data: %{
          run_ends: %Adbc.Column{
            name: "run_ends",
            type: :s32,
            nullable: false,
            metadata: nil,
            data: [2, 4, 7]
          },
          values: inner_run_end_array
        }
      }

      assert Adbc.Column.to_list(run_end_array) == [1, 2, 2]
    end
  end

  describe "dictionary" do
    test "to list" do
      # type: VarBinary
      # ['foo', 'bar', 'foo', 'bar', null, 'baz']
      #
      # In dictionary-encoded form, this could appear as:
      # data VarBinary (dictionary-encoded)
      #    index_type: Int32
      #    values: [0, 1, 0, 1, null, 2]
      #
      # dictionary
      #    type: VarBinary
      #    values: ['foo', 'bar', 'baz']
      key = Adbc.Column.s32([0, 1, 0, 1, nil, 2], name: "key", nullable: true)
      value = Adbc.Column.string(["foo", "bar", "baz"], name: "value", nullable: false)
      dict = Adbc.Column.dictionary(key, value)

      assert Adbc.Column.to_list(dict) == ["foo", "bar", "foo", "bar", nil, "baz"]
    end
  end

  describe "struct" do
    test "to list" do
      struct = %Adbc.Column{
        name: "struct",
        type:
          {:struct,
           [
             %Adbc.Column{
               name: "val1",
               type: :s64,
               nullable: true,
               metadata: nil,
               data: nil,
               length: nil,
               offset: nil
             },
             %Adbc.Column{
               name: "val2",
               type: :string,
               nullable: true,
               metadata: nil,
               data: nil,
               length: nil,
               offset: nil
             }
           ]},
        nullable: true,
        metadata: nil,
        data: [
          %Adbc.Column{
            name: "val1",
            type: :s64,
            nullable: true,
            metadata: nil,
            data: [298_258_424, 162_342_654],
            length: nil,
            offset: nil
          },
          %Adbc.Column{
            name: "val2",
            type: :string,
            nullable: true,
            metadata: nil,
            data: ["hello world", "hello elixir"],
            length: nil,
            offset: nil
          }
        ],
        length: nil,
        offset: nil
      }

      assert Adbc.Column.to_list(struct) == [
               %{"val1" => 298_258_424, "val2" => "hello world"},
               %{"val1" => 162_342_654, "val2" => "hello elixir"}
             ]
    end
  end
end
