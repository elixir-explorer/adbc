defmodule Adbc.Column do
  @moduledoc """
  Documentation for `Adbc.Column`.

  `Adbc.Column` corresponds to a column in the table. It contains the column's name, type, and
  data. The data is a list of values of the column's data type.
  """
  defstruct name: nil,
            type: nil,
            nullable: false,
            metadata: %{},
            data: nil

  @type i8 :: -128..127
  @type u8 :: 0..255
  @type i16 :: -32768..32767
  @type u16 :: 0..65535
  @type i32 :: -2_147_483_648..2_147_483_647
  @type u32 :: 0..4_294_967_295
  @type i64 :: -9_223_372_036_854_775_808..9_223_372_036_854_775_807
  @type u64 :: 0..18_446_744_073_709_551_615
  @type signed_integer ::
          :i8
          | :i16
          | :i32
          | :i64
  @type unsigned_integer ::
          :u8
          | :u16
          | :u32
          | :u64
  @type floating ::
          :f32
          | :f64
  @type precision128 :: 1..38
  @type precision256 :: 1..76
  @type decimal128 :: {:decimal, 128, precision128(), integer()}
  @type decimal256 :: {:decimal, 256, precision256(), integer()}
  @type decimal_t ::
          decimal128
          | decimal256
  @type time_unit ::
          :seconds
          | :milliseconds
          | :microseconds
          | :nanoseconds
  @type time32_t ::
          {:time32, :seconds}
          | {:time32, :milliseconds}
  @type time64_t ::
          {:time64, :microseconds}
          | {:time64, :nanoseconds}
  @type time_t :: time32_t() | time64_t()
  @type timestamp_t ::
          {:timestamp, :seconds, String.t()}
          | {:timestamp, :milliseconds, String.t()}
          | {:timestamp, :microseconds, String.t()}
          | {:timestamp, :nanoseconds, String.t()}
  @type duration_t ::
          {:duration, :seconds}
          | {:duration, :milliseconds}
          | {:duration, :microseconds}
          | {:duration, :nanoseconds}
  @type interval_month :: i32()
  @type interval_day_time :: {i32(), i32()}
  @type interval_month_day_nano :: {i32(), i32(), i64()}
  @type interval_unit ::
          :month
          | :day_time
          | :month_day_nano
  @type interval_t ::
          {:interval, :month}
          | {:interval, :day_time}
          | {:interval, :month_day_nano}
  @type data_type ::
          :boolean
          | signed_integer
          | unsigned_integer
          | floating
          | decimal_t
          | :string
          | :large_string
          | :binary
          | :large_binary
          | {:fixed_size_binary, non_neg_integer()}
          | :date32
          | :date64
          | time_t
          | timestamp_t
          | duration_t
          | interval_t

  @spec column(data_type(), list, Keyword.t()) :: %Adbc.Column{}
  def column(type, data, opts \\ [])
      when (is_atom(type) or is_tuple(type)) and is_list(data) and is_list(opts) do
    name = opts[:name]
    nullable = opts[:nullable] || false
    metadata = opts[:metadata]

    %Adbc.Column{
      name: name,
      type: type,
      nullable: nullable,
      metadata: metadata,
      data: data
    }
  end

  @spec get_metadata(%Adbc.Column{}, String.t(), String.t() | nil) :: String.t() | nil
  def get_metadata(%Adbc.Column{metadata: metadata}, key, default \\ nil)
      when is_binary(key) or is_atom(key) do
    metadata[to_string(key)] || default
  end

  @spec set_metadata(%Adbc.Column{}, String.t(), String.t()) :: %Adbc.Column{}
  def set_metadata(buffer = %Adbc.Column{metadata: metadata}, key, value)
      when (is_binary(key) or is_atom(key)) and (is_binary(value) or is_atom(value)) do
    %Adbc.Column{buffer | metadata: Map.put(metadata, to_string(key), to_string(value))}
  end

  @spec delete_metadata(%Adbc.Column{}, String.t()) :: %Adbc.Column{}
  def delete_metadata(buffer = %Adbc.Column{metadata: metadata}, key)
      when is_binary(key) or is_atom(key) do
    %Adbc.Column{buffer | metadata: Map.delete(metadata, to_string(key))}
  end

  @spec delete_all_metadata(%Adbc.Column{}) :: %Adbc.Column{}
  def delete_all_metadata(buffer = %Adbc.Column{}) do
    %Adbc.Column{buffer | metadata: %{}}
  end

  @doc """
  A column that contains booleans.

  ## Arguments

  * `data`: A list of booleans
  * `opts`: A keyword list of options

  ## Options

  * `:name` - The name of the column
  * `:nullable` - A boolean value indicating whether the column is nullable
  * `:metadata` - A map of metadata

  ## Examples

      iex> Adbc.Column.boolean([true, false, true])
      %Adbc.Column{
        name: nil,
        type: :boolean,
        nullable: false,
        metadata: %{},
        data: [true, false, true]
      }

  """
  @spec boolean([boolean()], Keyword.t()) :: %Adbc.Column{}
  def boolean(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:boolean, data, opts)
  end

  @doc """
  A column that contains unsigned 8-bit integers.

  ## Arguments

  * `data`: A list of unsigned 8-bit integer values
  * `opts`: A keyword list of options

  ## Options

  * `:name` - The name of the column
  * `:nullable` - A boolean value indicating whether the column is nullable
  * `:metadata` - A map of metadata

  ## Examples

      iex> Adbc.Column.u8([1, 2, 3])
      %Adbc.Column{
        name: nil,
        type: :u8,
        nullable: false,
        metadata: %{},
        data: [1, 2, 3]
      }

  """
  @spec u8([u8() | nil], Keyword.t()) :: %Adbc.Column{}
  def u8(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:u8, data, opts)
  end

  @doc """
  A column that contains unsigned 16-bit integers.

  ## Arguments

  * `data`: A list of unsigned 16-bit integer values
  * `opts`: A keyword list of options

  ## Options

  * `:name` - The name of the column
  * `:nullable` - A boolean value indicating whether the column is nullable
  * `:metadata` - A map of metadata

  ## Examples

      iex> Adbc.Column.u16([1, 2, 3])
      %Adbc.Column{
        name: nil,
        type: :u16,
        nullable: false,
        metadata: %{},
        data: [1, 2, 3]
      }

  """
  @spec u16([u16() | nil], Keyword.t()) :: %Adbc.Column{}
  def u16(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:u16, data, opts)
  end

  @doc """
  A column that contains un32-bit signed integers.

  ## Arguments

  * `data`: A list of un32-bit signed integer values
  * `opts`: A keyword list of options

  ## Options

  * `:name` - The name of the column
  * `:nullable` - A boolean value indicating whether the column is nullable
  * `:metadata` - A map of metadata

  ## Examples

      iex> Adbc.Column.u32([1, 2, 3])
      %Adbc.Column{
        name: nil,
        type: :u32,
        nullable: false,
        metadata: %{},
        data: [1, 2, 3]
      }

  """
  @spec u32([u32() | nil], Keyword.t()) :: %Adbc.Column{}
  def u32(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:u32, data, opts)
  end

  @doc """
  A column that contains un64-bit signed integers.

  ## Arguments

  * `data`: A list of un64-bit signed integer values
  * `opts`: A keyword list of options

  ## Options

  * `:name` - The name of the column
  * `:nullable` - A boolean value indicating whether the column is nullable
  * `:metadata` - A map of metadata

  ## Examples

      iex> Adbc.Column.u32([1, 2, 3])
      %Adbc.Column{
        name: nil,
        type: :u32,
        nullable: false,
        metadata: %{},
        data: [1, 2, 3]
      }

  """
  @spec u64([u64() | nil], Keyword.t()) :: %Adbc.Column{}
  def u64(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:u64, data, opts)
  end

  @doc """
  A column that contains signed 8-bit integers.

  ## Arguments

  * `data`: A list of signed 8-bit integer values
  * `opts`: A keyword list of options

  ## Options

  * `:name` - The name of the column
  * `:nullable` - A boolean value indicating whether the column is nullable
  * `:metadata` - A map of metadata

  ## Examples

      iex> Adbc.Column.i8([1, 2, 3])
      %Adbc.Column{
        name: nil,
        type: :i8,
        nullable: false,
        metadata: %{},
        data: [1, 2, 3]
      }

  """
  @spec i8([i8() | nil], Keyword.t()) :: %Adbc.Column{}
  def i8(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:i8, data, opts)
  end

  @doc """
  A column that contains signed 16-bit integers.

  ## Arguments

  * `data`: A list of signed 16-bit integer values
  * `opts`: A keyword list of options

  ## Options

  * `:name` - The name of the column
  * `:nullable` - A boolean value indicating whether the column is nullable
  * `:metadata` - A map of metadata

  ## Examples

      iex> Adbc.Column.i16([1, 2, 3])
      %Adbc.Column{
        name: nil,
        type: :i16,
        nullable: false,
        metadata: %{},
        data: [1, 2, 3]
      }

  """
  @spec i16([i16() | nil], Keyword.t()) :: %Adbc.Column{}
  def i16(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:i16, data, opts)
  end

  @doc """
  A column that contains 32-bit signed integers.

  ## Arguments

  * `data`: A list of 32-bit signed integer values
  * `opts`: A keyword list of options

  ## Options

  * `:name` - The name of the column
  * `:nullable` - A boolean value indicating whether the column is nullable
  * `:metadata` - A map of metadata

  ## Examples

      iex> Adbc.Column.i32([1, 2, 3])
      %Adbc.Column{
        name: nil,
        type: :i32,
        nullable: false,
        metadata: %{},
        data: [1, 2, 3]
      }

  """
  @spec i32([i32() | nil], Keyword.t()) :: %Adbc.Column{}
  def i32(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:i32, data, opts)
  end

  @doc """
  A column that contains 64-bit signed integers.

  ## Arguments

  * `data`: A list of 64-bit signed integer values
  * `opts`: A keyword list of options

  ## Options

  * `:name` - The name of the column
  * `:nullable` - A boolean value indicating whether the column is nullable
  * `:metadata` - A map of metadata

  ## Examples

      iex> Adbc.Column.i64([1, 2, 3])
      %Adbc.Column{
        name: nil,
        type: :i64,
        nullable: false,
        metadata: %{},
        data: [1, 2, 3]
      }

  """
  @spec i64([i64() | nil], Keyword.t()) :: %Adbc.Column{}
  def i64(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:i64, data, opts)
  end

  @doc """
  A column that contains 16-bit half-precision floats.

  ## Arguments

  * `data`: A list of 32-bit single-precision float values (will be converted to 16-bit floats in C)
  * `opts`: A keyword list of options

  ## Options

  * `:name` - The name of the column
  * `:nullable` - A boolean value indicating whether the column is nullable
  * `:metadata` - A map of metadata

  ## Examples

      iex> Adbc.Column.f16([1.0, 2.0, 3.0])
      %Adbc.Column{
        name: nil,
        type: :f16,
        nullable: false,
        metadata: %{},
        data: [1.0, 2.0, 3.0]
      }

  """
  @spec f16([float | nil | :infinity | :neg_infinity | :nan], Keyword.t()) :: %Adbc.Column{}
  def f16(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:f16, data, opts)
  end

  @doc """
  A column that contains 32-bit single-precision floats.

  ## Arguments

  * `data`: A list of 32-bit single-precision float values
  * `opts`: A keyword list of options

  ## Options

  * `:name` - The name of the column
  * `:nullable` - A boolean value indicating whether the column is nullable
  * `:metadata` - A map of metadata

  ## Examples

      iex> Adbc.Column.f32([1.0, 2.0, 3.0])
      %Adbc.Column{
        name: nil,
        type: :f32,
        nullable: false,
        metadata: %{},
        data: [1.0, 2.0, 3.0]
      }

  """
  @spec f32([float | nil | :infinity | :neg_infinity | :nan], Keyword.t()) :: %Adbc.Column{}
  def f32(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:f32, data, opts)
  end

  @doc """
  A column that contains 64-bit double-precision floats.

  ## Arguments

  * `data`: A list of 64-bit double-precision float values
  * `opts`: A keyword list of options

  ## Options

  * `:name` - The name of the column
  * `:nullable` - A boolean value indicating whether the column is nullable
  * `:metadata` - A map of metadata

  ## Examples

      iex> Adbc.Column.f64([1.0, 2.0, 3.0])
      %Adbc.Column{
        name: nil,
        type: :f64,
        nullable: false,
        metadata: %{},
        data: [1.0, 2.0, 3.0]
      }

  """
  @spec f64([float | nil | :infinity | :neg_infinity | :nan], Keyword.t()) :: %Adbc.Column{}
  def f64(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:f64, data, opts)
  end

  @doc """
  A column that contains 128-bit decimals.

  ## Arguments

  * `data`: a list, each element can be either
    * a `Decimal.t()`
    * an `integer()`
  * `precision`: The precision of the decimal values; precision should be between 1 and 38
  * `scale`: The scale of the decimal values
  * `opts`: A keyword list of options

  ## Options

  * `:name` - The name of the column
  * `:nullable` - A boolean value indicating whether the column is nullable
  * `:metadata` - A map of metadata
  """
  @spec decimal128([Decimal.t() | integer() | nil], precision128(), integer(), Keyword.t()) ::
          %Adbc.Column{}
  def decimal128(data, precision, scale, opts \\ [])
      when is_integer(precision) and precision >= 1 and precision <= 38 do
    bitwidth = 128

    column(
      {:decimal, bitwidth, precision, scale},
      preprocess_decimal(bitwidth, precision, scale, data, []),
      opts
    )
  end

  @doc """
  A column that contains 256-bit decimals.

  ## Arguments

  * `data`: a list, each element can be either
    * a `Decimal.t()`
    * an `integer()`
  * `precision`: The precision of the decimal values; precision should be between 1 and 76
  * `scale`: The scale of the decimal values
  * `opts`: A keyword list of options

  ## Options

  * `:name` - The name of the column
  * `:nullable` - A boolean value indicating whether the column is nullable
  * `:metadata` - A map of metadata
  """
  @spec decimal256([Decimal.t() | integer() | nil], precision256(), integer(), Keyword.t()) ::
          %Adbc.Column{}
  def decimal256(data, precision, scale, opts \\ [])
      when is_integer(precision) and precision >= 1 and precision <= 76 do
    bitwidth = 256

    column(
      {:decimal, bitwidth, precision, scale},
      preprocess_decimal(bitwidth, precision, scale, data, []),
      opts
    )
  end

  defp preprocess_decimal(_bitwidth, _precision, _scale, [], acc), do: Enum.reverse(acc)

  defp preprocess_decimal(bitwidth, precision, scale, [nil | rest], acc) do
    preprocess_decimal(bitwidth, precision, scale, rest, [nil | acc])
  end

  defp preprocess_decimal(bitwidth, precision, scale, [integer | rest], acc)
       when is_integer(integer) do
    if Decimal.coef_length(integer) > precision do
      raise Adbc.Error,
            "`#{Integer.to_string(integer)}` cannot be fitted into a decimal#{Integer.to_string(bitwidth)} with the specified precision #{Integer.to_string(precision)}"
    else
      coef = trunc(integer * :math.pow(10, scale))
      acc = [<<coef::signed-integer-little-size(bitwidth)>> | acc]
      preprocess_decimal(bitwidth, precision, scale, rest, acc)
    end
  end

  defp preprocess_decimal(
         bitwidth,
         _precision,
         scale,
         [%Decimal{exp: exp} = decimal | _rest],
         _acc
       )
       when -exp > scale do
    raise Adbc.Error,
          "`#{Decimal.to_string(decimal)}` with exponent `#{exp}` cannot be represented as a valid decimal#{Integer.to_string(bitwidth)} number with scale value `#{scale}`"
  end

  defp preprocess_decimal(bitwidth, precision, scale, [%Decimal{exp: exp} = decimal | rest], acc)
       when -exp <= scale do
    if Decimal.inf?(decimal) or Decimal.nan?(decimal) do
      raise Adbc.Error,
            "`#{Decimal.to_string(decimal)}` cannot be represented as a valid decimal#{Integer.to_string(bitwidth)} number"
    else
      if Decimal.coef_length(decimal.coef) > precision do
        raise Adbc.Error,
              "`#{Decimal.to_string(decimal)}` cannot be fitted into a decimal#{Integer.to_string(bitwidth)} with the specified precision #{Integer.to_string(precision)}"
      else
        coef = trunc(decimal.coef * decimal.sign * :math.pow(10, exp + scale))
        acc = [<<coef::signed-integer-little-size(bitwidth)>> | acc]
        preprocess_decimal(bitwidth, precision, scale, rest, acc)
      end
    end
  end

  @doc """
  A column that contains UTF-8 encoded strings.

  ## Arguments

  * `data`: A list of UTF-8 encoded string values
  * `opts`: A keyword list of options

  ## Options

  * `:name` - The name of the column
  * `:nullable` - A boolean value indicating whether the column is nullable
  * `:metadata` - A map of metadata

  ## Examples

      iex> Adbc.Column.string(["a", "ab", "abc"])
      %Adbc.Column{
        name: nil,
        type: :string,
        nullable: false,
        metadata: %{},
        data: ["a", "ab", "abc"]
      }

  """
  @spec string([String.t() | nil], Keyword.t()) :: %Adbc.Column{}
  def string(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:string, data, opts)
  end

  @doc """
  A column that contains UTF-8 encoded large strings.

  Similar to `string/2`, but for strings larger than 2GB.

  ## Arguments

  * `data`: A list of UTF-8 encoded string values
  * `opts`: A keyword list of options

  ## Options

  * `:name` - The name of the column
  * `:nullable` - A boolean value indicating whether the column is nullable
  * `:metadata` - A map of metadata

  ## Examples

      iex> Adbc.Column.large_string(["a", "ab", "abc"])
      %Adbc.Column{
        name: nil,
        type: :large_string,
        nullable: false,
        metadata: %{},
        data: ["a", "ab", "abc"]
      }

  """
  @spec large_string([String.t() | nil], Keyword.t()) :: %Adbc.Column{}
  def large_string(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:large_string, data, opts)
  end

  @doc """
  A column that contains binary values.

  ## Arguments

  * `data`: A list of binary values
  * `opts`: A keyword list of options

  ## Options

  * `:name` - The name of the column
  * `:nullable` - A boolean value indicating whether the column is nullable
  * `:metadata` - A map of metadata

  ## Examples

      iex> Adbc.Column.binary([<<0>>, <<1>>, <<2>>])
      %Adbc.Column{
        name: nil,
        type: :binary,
        nullable: false,
        metadata: %{},
        data: [<<0>>, <<1>>, <<2>>]
      }

  """
  @spec binary([iodata() | nil], Keyword.t()) :: %Adbc.Column{}
  def binary(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:binary, data, opts)
  end

  @doc """
  A column that contains large binary values.

  Similar to `binary/2`, but for binary values larger than 2GB.

  ## Arguments

  * `data`: A list of binary values
  * `opts`: A keyword list of options

  ## Options

  * `:name` - The name of the column
  * `:nullable` - A boolean value indicating whether the column is nullable
  * `:metadata` - A map of metadata

  ## Examples

      iex> Adbc.Column.large_binary([<<0>>, <<1>>, <<2>>])
      %Adbc.Column{
        name: nil,
        type: :large_binary,
        nullable: false,
        metadata: %{},
        data: [<<0>>, <<1>>, <<2>>]
      }

  """
  @spec large_binary([iodata() | nil], Keyword.t()) :: %Adbc.Column{}
  def large_binary(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:large_binary, data, opts)
  end

  @doc """
  A column that contains fixed size binaries.

  Similar to `binary/2`, but each binary value has the same fixed size in bytes.

  ## Arguments

  * `data`: A list of binary values
  * `nbytes`: The fixed size of the binary values in bytes
  * `opts`: A keyword list of options

  ## Options

  * `:name` - The name of the column
  * `:nullable` - A boolean value indicating whether the column is nullable
  * `:metadata` - A map of metadata

  ## Examples

      iex> Adbc.Column.fixed_size_binary([<<0>>, <<1>>, <<2>>], 1)
      %Adbc.Column{
        name: nil,
        type: {:fixed_size_binary, 1},
        nullable: false,
        metadata: %{},
        data: [<<0>>, <<1>>, <<2>>]
      }

  """
  @spec fixed_size_binary([iodata() | nil], non_neg_integer(), Keyword.t()) :: %Adbc.Column{}
  def fixed_size_binary(data, nbytes, opts \\ []) when is_list(data) and is_list(opts) do
    column({:fixed_size_binary, nbytes}, data, opts)
  end

  @doc """
  A column that contains date represented as 32-bit signed integers in UTC.
  ## Arguments

  * `data`: a list, each element of which can be one of the following:
    * a `Date.t()`
    * a 32-bit signed integer representing the number of days since the Unix epoch.
  * `opts`: A keyword list of options

  ## Options

  * `:name` - The name of the column
  * `:nullable` - A boolean value indicating whether the column is nullable
  * `:metadata` - A map of metadata
  """
  @spec date32([Date.t() | i32() | nil], Keyword.t()) :: %Adbc.Column{}
  def date32(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:date32, data, opts)
  end

  @doc """
  A column that contains date represented as 64-bit signed integers in UTC.
  ## Arguments

  * `data`: a list, each element of which can be one of the following:
    * a `Date.t()`
    * a 64-bit signed integer representing the number of milliseconds since the Unix epoch.
  * `opts`: A keyword list of options

  ## Options

  * `:name` - The name of the column
  * `:nullable` - A boolean value indicating whether the column is nullable
  * `:metadata` - A map of metadata
  """
  @spec date64([Date.t() | i64() | nil], Keyword.t()) :: %Adbc.Column{}
  def date64(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:date64, data, opts)
  end

  @doc """
  A column that contains time represented as signed integers in UTC.
  ## Arguments

  * `data`:
    * a list of `Time.t()` value
    * a list of integer values representing the time in the specified unit

      Note that when using `:seconds` or `:milliseconds` as the unit,
      the time value is limited to the range of 32-bit signed integers.

      For `:microseconds` and `:nanoseconds`, the time value is limited to the range of 64-bit signed integers.

  * `unit`: specify the unit of the time value, one of the following:
    * `:seconds`
    * `:milliseconds`
    * `:microseconds`
    * `:nanoseconds`

  * `opts`: A keyword list of options

  ## Options

  * `:name` - The name of the column
  * `:nullable` - A boolean value indicating whether the column is nullable
  * `:metadata` - A map of metadata
  """
  @spec time([Time.t() | nil] | [i64() | nil], time_unit(), Keyword.t()) :: %Adbc.Column{}
  def time(data, unit, opts \\ [])

  def time(data, :seconds, opts) when is_list(data) and is_list(opts) do
    column({:time32, :seconds}, data, opts)
  end

  def time(data, :milliseconds, opts) when is_list(data) and is_list(opts) do
    column({:time32, :milliseconds}, data, opts)
  end

  def time(data, :microseconds, opts) when is_list(data) and is_list(opts) do
    column({:time64, :microseconds}, data, opts)
  end

  def time(data, :nanoseconds, opts) when is_list(data) and is_list(opts) do
    column({:time64, :nanoseconds}, data, opts)
  end

  @doc """
  A column that contains timestamps represented as signed integers in the given timezone.

  ## Arguments

  * `data`:
    * a list of `NaiveDateTime.t()` value
    * a list of 64-bit signed integer values representing the time in the specified unit

  * `unit`: specify the unit of the time value, one of the following:
    * `:seconds`
    * `:milliseconds`
    * `:microseconds`
    * `:nanoseconds`

  * `timezone`: the timezone of the timestamp

  * `opts`: A keyword list of options

  ## Options

  * `:name` - The name of the column
  * `:nullable` - A boolean value indicating whether the column is nullable
  * `:metadata` - A map of metadata
  """
  @spec timestamp([NaiveDateTime.t() | nil] | [i64() | nil], time_unit(), String.t(), Keyword.t()) ::
          %Adbc.Column{}
  def timestamp(data, unit, timezone, opts \\ [])

  def timestamp(data, :seconds, timezone, opts)
      when is_list(data) and is_binary(timezone) and is_list(opts) do
    column({:timestamp, :seconds, timezone}, data, opts)
  end

  def timestamp(data, :milliseconds, timezone, opts)
      when is_list(data) and is_binary(timezone) and is_list(opts) do
    column({:timestamp, :milliseconds, timezone}, data, opts)
  end

  def timestamp(data, :microseconds, timezone, opts)
      when is_list(data) and is_binary(timezone) and is_list(opts) do
    column({:timestamp, :microseconds, timezone}, data, opts)
  end

  def timestamp(data, :nanoseconds, timezone, opts)
      when is_list(data) and is_binary(timezone) and is_list(opts) do
    column({:timestamp, :nanoseconds, timezone}, data, opts)
  end

  @doc """
  A column that contains durations represented as 64-bit signed integers.

  ## Arguments

  * `data`: a list of integer values representing the time in the specified unit

  * `unit`: specify the unit of the time value, one of the following:
    * `:seconds`
    * `:milliseconds`
    * `:microseconds`
    * `:nanoseconds`

  * `opts`: A keyword list of options

  ## Options

  * `:name` - The name of the column
  * `:nullable` - A boolean value indicating whether the column is nullable
  * `:metadata` - A map of metadata
  """
  @spec duration([i64() | nil], time_unit(), Keyword.t()) :: %Adbc.Column{}
  def duration(data, unit, opts \\ [])

  def duration(data, :seconds, opts) when is_list(data) and is_list(opts) do
    column({:duration, :seconds}, data, opts)
  end

  def duration(data, :milliseconds, opts) when is_list(data) and is_list(opts) do
    column({:duration, :milliseconds}, data, opts)
  end

  def duration(data, :microseconds, opts) when is_list(data) and is_list(opts) do
    column({:duration, :microseconds}, data, opts)
  end

  def duration(data, :nanoseconds, opts) when is_list(data) and is_list(opts) do
    column({:duration, :nanoseconds}, data, opts)
  end

  @doc """
  A column that contains durations represented as signed integers.

  ## Arguments

  * `data`: a list, each element of which can be one of the following:
    - if `unit` is `:month`:
      * a 32-bit signed integer representing the number of months.

    - if `unit` is `:day_time`:
      * a 2-tuple, both the number of days and the number of milliseconds are in 32-bit signed integers.

    - if `unit` is `:month_day_nano`:
      * a 3-tuple, the number of months, days, and nanoseconds;
        the number of months and days are in 32-bit signed integers,
        and the number of nanoseconds is in 64-bit signed integers

  * `unit`: specify the unit of the time value, one of the following:
    * `:month`
    * `:day_time`
    * `:month_day_nano`

  * `opts`: A keyword list of options

  ## Options

  * `:name` - The name of the column
  * `:nullable` - A boolean value indicating whether the column is nullable
  * `:metadata` - A map of metadata
  """
  @spec interval(
          [interval_month() | interval_day_time() | interval_month_day_nano() | nil],
          interval_unit(),
          Keyword.t()
        ) ::
          %Adbc.Column{}
  def interval(data, interval_unit, opts \\ [])

  def interval(data, :month, opts) do
    column({:interval, :month}, data, opts)
  end

  def interval(data, :day_time, opts) do
    column({:interval, :day_time}, data, opts)
  end

  def interval(data, :month_day_nano, opts) do
    column({:interval, :month_day_nano}, data, opts)
  end

  @doc """
  A column that each row is a list of some type or nil.

  ## Arguments

  * `data`: a list, each element of which can be one of the following:
    - `nil`
    - `Adbc.Column`

    Note that each `Adbc.Column` in the list should have the same type.

  * `opts`: A keyword list of options

  ## Options

  * `:name` - The name of the column
  * `:nullable` - A boolean value indicating whether the column is nullable
  * `:metadata` - A map of metadata
  """
  @spec list([%Adbc.Column{} | nil], Keyword.t()) :: %Adbc.Column{}
  def list(data, opts \\ []) when is_list(data) do
    column(:list, data, opts)
  end

  @doc """
  Similar to `list/2`, but for large lists.

  ## Arguments

  * `data`: a list, each element of which can be one of the following:
    - `nil`
    - `Adbc.Column`

    Note that each `Adbc.Column` in the list should have the same type.

  * `opts`: A keyword list of options

  ## Options

  * `:name` - The name of the column
  * `:nullable` - A boolean value indicating whether the column is nullable
  * `:metadata` - A map of metadata
  """
  @spec large_list([%Adbc.Column{} | nil], Keyword.t()) :: %Adbc.Column{}
  def large_list(data, opts \\ []) when is_list(data) do
    column(:large_list, data, opts)
  end

  @doc """
  Similar to `list/2`, but the length of the list is the same.

  ## Arguments

  * `data`: a list, each element of which can be one of the following:
    - `nil`
    - `Adbc.Column`

    Note that each `Adbc.Column` in the list should have the same type and length.

  * `fixed_size`: The fixed size of the list.

  * `opts`: A keyword list of options

  ## Options

  * `:name` - The name of the column
  * `:nullable` - A boolean value indicating whether the column is nullable
  * `:metadata` - A map of metadata
  """
  @spec fixed_size_list([%Adbc.Column{} | nil], [i32()], Keyword.t()) ::
          %Adbc.Column{}
  def fixed_size_list(data, fixed_size, opts \\ []) when is_list(data) do
    column({:fixed_size_list, fixed_size}, data, opts)
  end
end
