defmodule Adbc.ArrowArray do
  @moduledoc """
  Documentation for `Adbc.ArrowArray`.
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

  @spec get_pointer(Adbc.ArrowArray.t()) :: binary()
  def get_pointer(self = %T{}) do
    Adbc.Nif.adbc_arrow_array_get_pointer(self.reference)
  end
end
