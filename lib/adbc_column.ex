defmodule Adbc.Column do
  @moduledoc """
  Documentation for `Adbc.Column`.

  `Adbc.Column` corresponds to a column in the table. It contains the column's name, type, and
  data. The data is a list of values of the column's type. The type can be one of the following:

  * `:boolean`
  * `:u8`
  * `:u16`
  * `:u32`
  * `:u64`
  * `:i8`
  * `:i16`
  * `:i32`
  * `:i64`
  * `:f32`
  * `:f64`
  * `:string`
  * `:large_string`, when the size of the string is larger than 4GB
  * `:binary`
  * `:large_binary`, when the size of the binary is larger than 4GB
  * `:fixed_size_binary`
  """
  defstruct name: nil,
            type: nil,
            nullable: false,
            metadata: %{},
            data: nil

  @spec column(atom, list, Keyword.t()) :: %Adbc.Column{}
  def column(type, data, opts \\ []) when is_atom(type) and is_list(data) and is_list(opts) do
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

  @spec boolean([boolean()], Keyword.t()) :: %Adbc.Column{}
  def boolean(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:boolean, data, opts)
  end

  @doc """
  A buffer for an unsigned 8-bit integer column.

  ## Arguments

  * `data`:
    * A list of unsigned 8-bit integer values
    * A single binary type value where each byte represents an unsigned 8-bit integer
  * `opts` - A keyword list of options

  ## Options

  * `:name` - The name of the column
  * `:nullable` - A boolean value indicating whether the column is nullable
  * `:null_count` - The number of null values in the column, defaults to 0, only used when
    `:nullable` is `true`
  * `:null`: only used when `:nullable` is `true`, can be one of the following:
    * A list of booleans with the same length as  indicating whether each value is null
    * A list of non-negative integers where each integer represents the corresponding index of a
      null value
    * A single binary type value where each bit indicates whether each value is null
  * `:metadata` - A map of metadata

  ## Examples

      iex> Adbc.Buffer.u8([1, 2, 3])
      %Adbc.Column{
        name: nil,
        type: :u8,
        nullable: false,
        null_count: 0,
        null: nil,
        metadata: %{},
        data: [1, 2, 3]
      }
  """
  @spec u8([0..255] | binary(), Keyword.t()) :: %Adbc.Column{}
  def u8(u8, opts \\ [])

  def u8(data, opts) when is_list(data) and is_list(opts) do
    column(:u8, data, opts)
  end

  def u8(data, opts) when is_binary(data) and is_list(opts) do
    column(:u8, data, opts)
  end

  @spec u16([0..65535], Keyword.t()) :: %Adbc.Column{}
  def u16(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:u16, data, opts)
  end

  @spec u32([0..4_294_967_295], Keyword.t()) :: %Adbc.Column{}
  def u32(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:u32, data, opts)
  end

  @spec u64([0..18_446_744_073_709_551_615], Keyword.t()) :: %Adbc.Column{}
  def u64(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:u64, data, opts)
  end

  @spec i8([-128..127], Keyword.t()) :: %Adbc.Column{}
  def i8(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:i8, data, opts)
  end

  @spec i16([-32768..32767], Keyword.t()) :: %Adbc.Column{}
  def i16(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:i16, data, opts)
  end

  @spec i32([-2_147_483_648..2_147_483_647], Keyword.t()) :: %Adbc.Column{}
  def i32(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:i32, data, opts)
  end

  @spec i64([-9_223_372_036_854_775_808..9_223_372_036_854_775_807], Keyword.t()) ::
          %Adbc.Column{}
  def i64(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:i64, data, opts)
  end

  @spec f32([float], Keyword.t()) :: %Adbc.Column{}
  def f32(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:f32, data, opts)
  end

  @spec f64([float], Keyword.t()) :: %Adbc.Column{}
  def f64(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:f64, data, opts)
  end

  @spec string([String.t()], Keyword.t()) :: %Adbc.Column{}
  def string(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:string, data, opts)
  end

  @spec large_string([String.t()], Keyword.t()) :: %Adbc.Column{}
  def large_string(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:large_string, data, opts)
  end

  @spec binary([binary()], Keyword.t()) :: %Adbc.Column{}
  def binary(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:binary, data, opts)
  end

  @spec large_binary([binary()], Keyword.t()) :: %Adbc.Column{}
  def large_binary(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:large_binary, data, opts)
  end

  @spec fixed_size_binary([binary()], Keyword.t()) :: %Adbc.Column{}
  def fixed_size_binary(data, opts \\ []) when is_list(data) and is_list(opts) do
    column(:fixed_size_binary, data, opts)
  end
end
