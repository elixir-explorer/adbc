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

  @spec get_pointer(Adbc.ArrowArrayStream.t()) :: non_neg_integer()
  def get_pointer(self = %T{}) do
    Adbc.Nif.adbc_arrow_array_stream_get_pointer(self.reference)
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
