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

  def new do
    case Adbc.Nif.adbc_arrow_array_stream_new() do
      {:ok, ref, ptr} ->
        %T{
          reference: ref,
          pointer: ptr
        }

      {:error, reason} ->
        {:error, reason}
    end
  end
end
