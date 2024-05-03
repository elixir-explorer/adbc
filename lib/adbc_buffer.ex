defmodule Adbc.Buffer do
  defstruct [
    name: nil,
    type: nil,
    nullable: nil,
    metadata: nil,
    data: nil,
  ]

  @spec buffer(atom, list, Keyword.t()) :: %Adbc.Buffer{}
  def buffer(type, data, opts \\ []) when is_atom(type) and is_list(data) and is_list(opts) do
    name = opts[:name]
    nullable = opts[:nullable]
    metadata = opts[:metadata]
    %Adbc.Buffer{
      name: name,
      type: type,
      nullable: nullable,
      metadata: metadata,
      data: data
    }
  end

  @spec u8([0..255], Keyword.t()) :: %Adbc.Buffer{}
  def u8(data, opts \\ []) when is_list(data) and is_list(opts) do
    buffer(:u8, data, opts)
  end

  @spec u16([0..65535], Keyword.t()) :: %Adbc.Buffer{}
  def u16(data, opts \\ []) when is_list(data) and is_list(opts)  do
    buffer(:u16, data, opts)
  end

  @spec u32([0..4294967295], Keyword.t()) :: %Adbc.Buffer{}
  def u32(data, opts \\ []) when is_list(data) and is_list(opts) do
    buffer(:u32, data, opts)
  end

  @spec u64([0..18446744073709551615], Keyword.t()) :: %Adbc.Buffer{}
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

  @spec i32([-2147483648..2147483647], Keyword.t()) :: %Adbc.Buffer{}
  def i32(data, opts \\ []) when is_list(data) and is_list(opts) do
    buffer(:i32, data, opts)
  end

  @spec i64([-9223372036854775808..9223372036854775807], Keyword.t()) :: %Adbc.Buffer{}
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

  @spec binary([binary()], Keyword.t()) :: %Adbc.Buffer{}
  def binary(data, opts \\ []) when is_list(data) and is_list(opts) do
    buffer(:binary, data, opts)
  end
end
