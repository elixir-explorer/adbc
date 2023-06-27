defmodule Adbc.ArrowArrayStream do
  @moduledoc """
  Documentation for `Adbc.ArrowArrayStream`.
  """

  @typedoc """
  - **reference**: `reference`.

    The underlying erlang resource variable.

  """
  @type t :: %__MODULE__{
          reference: reference()
        }
  defstruct [:reference]
  alias __MODULE__, as: T
  alias Adbc.ArrowSchema

  @spec get_pointer(Adbc.ArrowArrayStream.t()) :: non_neg_integer()
  def get_pointer(self = %T{}) do
    Adbc.Nif.adbc_arrow_array_stream_get_pointer(self.reference)
  end

  @spec get_schema(Adbc.ArrowArrayStream.t()) ::
          {:ok, Adbc.ArrowSchema.t()} | {:error, String.t()} | Adbc.Error.adbc_error()
  def get_schema(self = %T{}) do
    case Adbc.Nif.adbc_arrow_array_stream_get_schema(self.reference) do
      {:ok, schema_ref, {format, name, metadata, flags, n_children, children}} ->
        {:ok,
         %ArrowSchema{
           format: format,
           name: name,
           metadata: metadata,
           flags: flags,
           n_children: n_children,
           children: Enum.map(children, &ArrowSchema.from_metainfo/1),
           reference: schema_ref
         }}

      other ->
        other
    end
  end

  @spec next(Adbc.ArrowArrayStream.t()) :: term() | {:error, String.t()}
  def next(self = %T{}) do
    Adbc.Nif.adbc_arrow_array_stream_next(self.reference)
  end

  @spec release(Adbc.ArrowArrayStream.t()) :: :ok | {:error, String.t()}
  def release(self = %T{}) do
    Adbc.Nif.adbc_arrow_array_stream_release(self.reference)
  end
end
