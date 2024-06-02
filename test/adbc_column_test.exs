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
               metadata: %{},
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
               metadata: %{},
               name: nil,
               nullable: false,
               type: {:decimal, ^bitwidth, ^precision, ^scale}
             } = Adbc.Column.decimal256([decimal, value], precision, scale)

      assert <<decode1::signed-integer-little-size(bitwidth)>> = decimal_data
      assert value == decode1 / :math.pow(10, scale)

      assert <<decode2::signed-integer-little-size(bitwidth)>> = value_data
      assert value == decode2 / :math.pow(10, scale)
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
               metadata: %{},
               name: nil,
               nullable: false,
               type: {:decimal, ^bitwidth, ^precision, ^scale}
             } = Adbc.Column.decimal128([decimal], precision, scale)

      assert <<decode::signed-integer-little-size(bitwidth)>> = data
      assert actual_value == decode / :math.pow(10, scale)

      bitwidth = 256

      assert %Adbc.Column{
               data: [data],
               metadata: %{},
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
               metadata: %{},
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
               metadata: %{},
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
               metadata: %{},
               name: nil,
               nullable: false,
               type: {:decimal, ^bitwidth, ^precision, ^scale}
             } = Adbc.Column.decimal128([decimal], precision, scale)

      assert <<decode::signed-integer-little-size(bitwidth)>> = data
      assert actual_value == decode / :math.pow(10, scale)

      bitwidth = 256

      assert %Adbc.Column{
               data: [data],
               metadata: %{},
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
        name: nil,
        type: :list_view,
        nullable: true,
        metadata: %{},
        data: %{
          values: %Adbc.Column{
            name: "item",
            type: :i32,
            nullable: false,
            metadata: nil,
            data: [0, -127, 127, 50, 12, -7, 25]
          },
          validity: [true, false, true, true, true],
          offsets: [4, 7, 0, 0, 3],
          sizes: [3, 0, 4, 0, 2]
        }
      }

      assert %Adbc.Column{
               name: nil,
               type: :list,
               nullable: true,
               metadata: %{},
               data: [
                 inner1 = %Adbc.Column{
                   name: "item",
                   type: :i32,
                   nullable: false,
                   metadata: nil,
                   data: [12, -7, 25]
                 },
                 _inner2 = nil,
                 inner3 = %Adbc.Column{
                   name: "item",
                   type: :i32,
                   nullable: false,
                   metadata: nil,
                   data: [0, -127, 127, 50]
                 },
                 inner4 = %Adbc.Column{
                   name: "item",
                   type: :i32,
                   nullable: false,
                   metadata: nil,
                   data: []
                 },
                 inner5 = %Adbc.Column{
                   name: "item",
                   type: :i32,
                   nullable: false,
                   metadata: nil,
                   data: ~c"2\f"
                 }
               ]
             } = Adbc.Column.list_view_to_list(list_view)

      nested_list_view = %Adbc.Column{
        name: nil,
        type: :list_view,
        nullable: true,
        metadata: %{},
        data: %{
          values: list_view,
          validity: [true, false, true, true, true],
          offsets: [2, 5, 0, 0, 3],
          sizes: [2, 0, 1, 0, 2]
        }
      }

      assert %Adbc.Column{
               data: [
                 # offsets=2, sizes=2
                 # => [inner3, inner4]
                 %Adbc.Column{
                   metadata: %{},
                   name: nil,
                   nullable: true,
                   type: :list,
                   data: [
                     ^inner3 = %Adbc.Column{
                       name: "item",
                       type: :i32,
                       nullable: false,
                       metadata: nil,
                       data: [0, -127, 127, 50]
                     },
                     ^inner4 = %Adbc.Column{
                       name: "item",
                       type: :i32,
                       nullable: false,
                       metadata: nil,
                       data: []
                     }
                   ]
                 },
                 # offsets=5, sizes=0
                 # => nil
                 nil,
                 # offsets=0, sizes=1
                 # => inner1
                 %Adbc.Column{
                   metadata: %{},
                   name: nil,
                   nullable: true,
                   type: :list,
                   data: [
                     ^inner1 = %Adbc.Column{
                       name: "item",
                       type: :i32,
                       nullable: false,
                       metadata: nil,
                       data: [12, -7, 25]
                     }
                   ]
                 },
                 # offsets=0, sizes=0
                 # => []
                 %Adbc.Column{data: [], metadata: %{}, name: nil, nullable: true, type: :list},
                 # offsets=3, sizes=2
                 # => [inner4, inner5]
                 %Adbc.Column{
                   data: [
                     ^inner4 = %Adbc.Column{
                       name: "item",
                       type: :i32,
                       nullable: false,
                       metadata: nil,
                       data: []
                     },
                     ^inner5 = %Adbc.Column{
                       name: "item",
                       type: :i32,
                       nullable: false,
                       metadata: nil,
                       data: ~c"2\f"
                     }
                   ],
                   metadata: %{},
                   name: nil,
                   nullable: true,
                   type: :list
                 }
               ],
               metadata: %{},
               name: nil,
               nullable: true,
               type: :list
             } = Adbc.Column.list_view_to_list(nested_list_view)
    end
  end
end
