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
  defstruct [:reference, :pointer]
  alias __MODULE__, as: T

  @doc """
  Create a new zero'ed struct ArrowArrayStream
  """
  def new do
    case Adbc.Nif.adbc_arrow_array_stream_new() do
      {:ok, ref, ptr} ->
        {:ok, %T{
          reference: ref,
          pointer: ptr
        }}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Reset the underlying C struct ArrowArrayStream to all zero
  so that it can be reused

  If the underlying C struct ArrowArrayStream has been set to
  store a previous error, ArrowArrayStream::release() will be
  called automatically before memset'ing it to zeros.
  """
  @spec reset(Adbc.ArrowArrayStream.t()) :: :ok | {:error, String.t()}
  def reset(self = %T{}) do
    Adbc.Nif.adbc_arrow_array_stream_reset(self.reference)
  end
end
