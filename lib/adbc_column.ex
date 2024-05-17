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
  @type decimal128 :: {:decimal, 128, integer(), integer()}
  @type decimal256 :: {:decimal, 256, integer(), integer()}
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
          | :fixed_size_binary
          | :date32
          | :date64
          | time32_t
          | time64_t
          | timestamp_t
          | duration_t

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

      iex> Adbc.Buffer.boolean([true, false, true])
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

      iex> Adbc.Buffer.u8([1, 2, 3])
      %Adbc.Column{
        name: nil,
        type: :u8,
        nullable: false,
        metadata: %{},
        data: [1, 2, 3]
      }

  """
  @spec u8([0..255], Keyword.t()) :: %Adbc.Column{}
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

      iex> Adbc.Buffer.u16([1, 2, 3])
      %Adbc.Column{
        name: nil,
        type: :u16,
        nullable: false,
        metadata: %{},
        data: [1, 2, 3]
      }

  """
  @spec u16([0..65535], Keyword.t()) :: %Adbc.Column{}
  def u16(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:u16, data, opts)
  end

  @doc """
  A column that contains unsigned 32-bit integers.

  ## Arguments

  * `data`: A list of unsigned 32-bit integer values
  * `opts`: A keyword list of options

  ## Options

  * `:name` - The name of the column
  * `:nullable` - A boolean value indicating whether the column is nullable
  * `:metadata` - A map of metadata

  ## Examples

      iex> Adbc.Buffer.u32([1, 2, 3])
      %Adbc.Column{
        name: nil,
        type: :u32,
        nullable: false,
        metadata: %{},
        data: [1, 2, 3]
      }

  """
  @spec u32([0..4_294_967_295], Keyword.t()) :: %Adbc.Column{}
  def u32(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:u32, data, opts)
  end

  @doc """
  A column that contains unsigned 64-bit integers.

  ## Arguments

  * `data`: A list of unsigned 64-bit integer values
  * `opts`: A keyword list of options

  ## Options

  * `:name` - The name of the column
  * `:nullable` - A boolean value indicating whether the column is nullable
  * `:metadata` - A map of metadata

  ## Examples

      iex> Adbc.Buffer.u32([1, 2, 3])
      %Adbc.Column{
        name: nil,
        type: :u32,
        nullable: false,
        metadata: %{},
        data: [1, 2, 3]
      }

  """
  @spec u64([0..18_446_744_073_709_551_615], Keyword.t()) :: %Adbc.Column{}
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

      iex> Adbc.Buffer.i8([1, 2, 3])
      %Adbc.Column{
        name: nil,
        type: :i8,
        nullable: false,
        metadata: %{},
        data: [1, 2, 3]
      }

  """
  @spec i8([-128..127], Keyword.t()) :: %Adbc.Column{}
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

      iex> Adbc.Buffer.i16([1, 2, 3])
      %Adbc.Column{
        name: nil,
        type: :i16,
        nullable: false,
        metadata: %{},
        data: [1, 2, 3]
      }

  """
  @spec i16([-32768..32767], Keyword.t()) :: %Adbc.Column{}
  def i16(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:i16, data, opts)
  end

  @doc """
  A column that contains signed 32-bit integers.

  ## Arguments

  * `data`: A list of signed 32-bit integer values
  * `opts`: A keyword list of options

  ## Options

  * `:name` - The name of the column
  * `:nullable` - A boolean value indicating whether the column is nullable
  * `:metadata` - A map of metadata

  ## Examples

      iex> Adbc.Buffer.i32([1, 2, 3])
      %Adbc.Column{
        name: nil,
        type: :i32,
        nullable: false,
        metadata: %{},
        data: [1, 2, 3]
      }

  """
  @spec i32([-2_147_483_648..2_147_483_647], Keyword.t()) :: %Adbc.Column{}
  def i32(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:i32, data, opts)
  end

  @doc """
  A column that contains signed 64-bit integers.

  ## Arguments

  * `data`: A list of signed 64-bit integer values
  * `opts`: A keyword list of options

  ## Options

  * `:name` - The name of the column
  * `:nullable` - A boolean value indicating whether the column is nullable
  * `:metadata` - A map of metadata

  ## Examples

      iex> Adbc.Buffer.i64([1, 2, 3])
      %Adbc.Column{
        name: nil,
        type: :i64,
        nullable: false,
        metadata: %{},
        data: [1, 2, 3]
      }

  """
  @spec i64([-9_223_372_036_854_775_808..9_223_372_036_854_775_807], Keyword.t()) ::
          %Adbc.Column{}
  def i64(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:i64, data, opts)
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

      iex> Adbc.Buffer.f32([1.0, 2.0, 3.0])
      %Adbc.Column{
        name: nil,
        type: :f32,
        nullable: false,
        metadata: %{},
        data: [1.0, 2.0, 3.0]
      }

  """
  @spec f32([float], Keyword.t()) :: %Adbc.Column{}
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

      iex> Adbc.Buffer.f64([1.0, 2.0, 3.0])
      %Adbc.Column{
        name: nil,
        type: :f64,
        nullable: false,
        metadata: %{},
        data: [1.0, 2.0, 3.0]
      }

  """
  @spec f64([float], Keyword.t()) :: %Adbc.Column{}
  def f64(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:f64, data, opts)
  end

  @doc """
  A column that contains 128-bit decimals.

  ## Arguments

  * `data`: a list, each element can be either
    * a `Decimal.t()`
    * an `integer()`
  * `precision`: The precision of the decimal values
  * `scale`: The scale of the decimal values
  * `opts`: A keyword list of options

  ## Options

  * `:name` - The name of the column
  * `:nullable` - A boolean value indicating whether the column is nullable
  * `:metadata` - A map of metadata
  """
  @spec decimal128([Decimal.t() | integer()], integer(), integer(), Keyword.t()) :: %Adbc.Column{}
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
  * `precision`: The precision of the decimal values
  * `scale`: The scale of the decimal values
  * `opts`: A keyword list of options

  ## Options

  * `:name` - The name of the column
  * `:nullable` - A boolean value indicating whether the column is nullable
  * `:metadata` - A map of metadata
  """
  @spec decimal256([Decimal.t() | integer()], integer(), integer(), Keyword.t()) :: %Adbc.Column{}
  def decimal256(data, precision, scale, opts \\ [])
      when is_integer(precision) and precision >= 1 and precision <= 38 do
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

      iex> Adbc.Buffer.string(["a", "ab", "abc"])
      %Adbc.Column{
        name: nil,
        type: :string,
        nullable: false,
        metadata: %{},
        data: ["a", "ab", "abc"]
      }

  """
  @spec string([String.t()], Keyword.t()) :: %Adbc.Column{}
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

      iex> Adbc.Buffer.large_string(["a", "ab", "abc"])
      %Adbc.Column{
        name: nil,
        type: :large_string,
        nullable: false,
        metadata: %{},
        data: ["a", "ab", "abc"]
      }

  """
  @spec large_string([String.t()], Keyword.t()) :: %Adbc.Column{}
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

      iex> Adbc.Buffer.binary([<<0>>, <<1>>, <<2>>])
      %Adbc.Column{
        name: nil,
        type: :binary,
        nullable: false,
        metadata: %{},
        data: [<<0>>, <<1>>, <<2>>]
      }

  """
  @spec binary([binary()], Keyword.t()) :: %Adbc.Column{}
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

      iex> Adbc.Buffer.large_binary([<<0>>, <<1>>, <<2>>])
      %Adbc.Column{
        name: nil,
        type: :large_binary,
        nullable: false,
        metadata: %{},
        data: [<<0>>, <<1>>, <<2>>]
      }

  """
  @spec large_binary([binary()], Keyword.t()) :: %Adbc.Column{}
  def large_binary(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:large_binary, data, opts)
  end

  @doc """
  A column that contains fixed size binaries.

  Similar to `binary/2`, but each binary value has the same fixed size in bytes.

  ## Arguments

  * `data`: A list of binary values
  * `opts`: A keyword list of options

  ## Options

  * `:name` - The name of the column
  * `:nullable` - A boolean value indicating whether the column is nullable
  * `:metadata` - A map of metadata

  ## Examples

      iex> Adbc.Buffer.fixed_size_binary([<<0>>, <<1>>, <<2>>])
      %Adbc.Column{
        name: nil,
        type: :fixed_size_binary,
        nullable: false,
        metadata: %{},
        data: [<<0>>, <<1>>, <<2>>]
      }

  """
  @spec fixed_size_binary([binary()], Keyword.t()) :: %Adbc.Column{}
  def fixed_size_binary(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:fixed_size_binary, data, opts)
  end

  @doc """
  A column that contains date represented as 32-bit integers in UTC.
  ## Arguments

  * `data`: a list, each element of which can be one of the following:
    * a `Date.t()`
    * a 32-bit integer representing the number of days since the Unix epoch.
  * `opts`: A keyword list of options

  ## Options

  * `:name` - The name of the column
  * `:nullable` - A boolean value indicating whether the column is nullable
  * `:metadata` - A map of metadata
  """
  @spec date32([Date.t() | integer()], Keyword.t()) :: %Adbc.Column{}
  def date32(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:date32, data, opts)
  end

  @doc """
  A column that contains date represented as 64-bit integers in UTC.
  ## Arguments

  * `data`: a list, each element of which can be one of the following:
    * a `Date.t()`
    * a 64-bit integer representing the number of milliseconds since the Unix epoch.
  * `opts`: A keyword list of options

  ## Options

  * `:name` - The name of the column
  * `:nullable` - A boolean value indicating whether the column is nullable
  * `:metadata` - A map of metadata
  """
  @spec date64([Date.t() | integer()], Keyword.t()) :: %Adbc.Column{}
  def date64(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:date64, data, opts)
  end

  @doc """
  A column that contains time represented as integers in UTC.
  ## Arguments

  * `data`:
    * a list of `Time.t()` value
    * a list of integer values representing the time in the specified unit

      Note that when using `:seconds` or `:milliseconds` as the unit,
      the time value is limited to the range of 32-bit integers.

      For `:microseconds` and `:nanoseconds`, the time value is limited to the range of 64-bit integers.

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
  @spec time([Time.t()] | [integer()], time_unit(), Keyword.t()) :: %Adbc.Column{}
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
  A column that contains timestamps represented as integers in the given timezone.

  ## Arguments

  * `data`:
    * a list of `NaiveDateTime.t()` value
    * a list of integer values representing the time in the specified unit

      Note that when using `:seconds` or `:milliseconds` as the unit,
      the time value is limited to the range of 32-bit integers.

      For `:microseconds` and `:nanoseconds`, the time value is limited to the range of 64-bit integers.

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
  @spec timestamp([NaiveDateTime.t()] | [integer()], time_unit(), String.t(), Keyword.t()) ::
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
  A column that contains durations represented as signed 64-bit integers.

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
  @spec duration([integer()], time_unit(), Keyword.t()) :: %Adbc.Column{}
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
end
