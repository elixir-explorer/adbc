defmodule Adbc.Buffer do
  @moduledoc """
  Documentation for `Adbc.Buffer`.

  One `Adbc.Buffer` corresponds to a column in the table. It contains the column's name, type, and
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

  @spec buffer(atom, list, Keyword.t()) :: %Adbc.Buffer{}
  def buffer(type, data, opts \\ []) when is_atom(type) and is_list(data) and is_list(opts) do
    name = opts[:name]
    nullable = opts[:nullable] || false
    metadata = opts[:metadata]

    %Adbc.Buffer{
      name: name,
      type: type,
      nullable: nullable,
      metadata: metadata,
      data: data
    }
  end

  @spec get_metadata(%Adbc.Buffer{}, String.t(), String.t()) :: String.t() | nil
  def get_metadata(%Adbc.Buffer{metadata: metadata}, key, default \\ nil)
      when is_binary(key) or is_atom(key) do
    metadata[to_string(key)] || default
  end

  @spec set_metadata(%Adbc.Buffer{}, String.t(), String.t()) :: %Adbc.Buffer{}
  def set_metadata(buffer = %Adbc.Buffer{metadata: metadata}, key, value)
      when (is_binary(key) or is_atom(key)) and (is_binary(value) or is_atom(value)) do
    %Adbc.Buffer{buffer | metadata: Map.put(metadata, to_string(key), to_string(value))}
  end

  @spec delete_metadata(%Adbc.Buffer{}, String.t()) :: %Adbc.Buffer{}
  def delete_metadata(buffer = %Adbc.Buffer{metadata: metadata}, key)
      when is_binary(key) or is_atom(key) do
    %Adbc.Buffer{buffer | metadata: Map.delete(metadata, to_string(key))}
  end

  @spec delete_all_metadata(%Adbc.Buffer{}) :: %Adbc.Buffer{}
  def delete_all_metadata(buffer = %Adbc.Buffer{})
    %Adbc.Buffer{buffer | metadata: %{}}
  end

  @spec boolean([0..255], Keyword.t()) :: %Adbc.Buffer{}
  def boolean(data, opts \\ []) when is_list(data) and is_list(opts) do
    buffer(:boolean, data, opts)
  end

  @spec u8([0..255], Keyword.t()) :: %Adbc.Buffer{}
  def u8(data, opts \\ []) when is_list(data) and is_list(opts) do
    buffer(:u8, data, opts)
  end

  @spec u16([0..65535], Keyword.t()) :: %Adbc.Buffer{}
  def u16(data, opts \\ []) when is_list(data) and is_list(opts) do
    buffer(:u16, data, opts)
  end

  @spec u32([0..4_294_967_295], Keyword.t()) :: %Adbc.Buffer{}
  def u32(data, opts \\ []) when is_list(data) and is_list(opts) do
    buffer(:u32, data, opts)
  end

  @spec u64([0..18_446_744_073_709_551_615], Keyword.t()) :: %Adbc.Buffer{}
  def u64(data, opts \\ []) when is_list(data) and is_list(opts) do
    buffer(:u64, data, opts)
  end

  @spec i8([-128..127], Keyword.t()) :: %Adbc.Buffer{}
  def i8(data, opts \\ []) when is_list(data) and is_list(opts) do
    buffer(:i8, data, opts)
  end

  @spec i16([-32768..32767], Keyword.t()) :: %Adbc.Buffer{}
  def i16(data, opts \\ []) when is_list(data) and is_list(opts) do
    buffer(:i16, data, opts)
  end

  @spec i32([-2_147_483_648..2_147_483_647], Keyword.t()) :: %Adbc.Buffer{}
  def i32(data, opts \\ []) when is_list(data) and is_list(opts) do
    buffer(:i32, data, opts)
  end

  @spec i64([-9_223_372_036_854_775_808..9_223_372_036_854_775_807], Keyword.t()) ::
          %Adbc.Buffer{}
  def i64(data, opts \\ []) when is_list(data) and is_list(opts) do
    buffer(:i64, data, opts)
  end

  @spec f32([float], Keyword.t()) :: %Adbc.Buffer{}
  def f32(data, opts \\ []) when is_list(data) and is_list(opts) do
    buffer(:f32, data, opts)
  end

  @spec f64([float], Keyword.t()) :: %Adbc.Buffer{}
  def f64(data, opts \\ []) when is_list(data) and is_list(opts) do
    buffer(:f64, data, opts)
  end

  @spec string([String.t()], Keyword.t()) :: %Adbc.Buffer{}
  def string(data, opts \\ []) when is_list(data) and is_list(opts) do
    buffer(:string, data, opts)
  end

  @spec large_string([String.t()], Keyword.t()) :: %Adbc.Buffer{}
  def large_string(data, opts \\ []) when is_list(data) and is_list(opts) do
    buffer(:large_string, data, opts)
  end

  @spec binary([binary()], Keyword.t()) :: %Adbc.Buffer{}
  def binary(data, opts \\ []) when is_list(data) and is_list(opts) do
    buffer(:binary, data, opts)
  end

  @spec large_binary([binary()], Keyword.t()) :: %Adbc.Buffer{}
  def large_binary(data, opts \\ []) when is_list(data) and is_list(opts) do
    buffer(:large_binary, data, opts)
  end

  @spec fixed_size_binary([binary()], Keyword.t()) :: %Adbc.Buffer{}
  def fixed_size_binary(data, opts \\ []) when is_list(data) and is_list(opts) do
    buffer(:fixed_size_binary, data, opts)
  end
end
