defmodule Adbc.ArrowSchema do
  @moduledoc """
  Documentation for `Adbc.ArrowSchema`.
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

  @spec get_pointer(Adbc.ArrowSchema.t()) :: binary()
  def get_pointer(self = %T{}) do
    Adbc.Nif.adbc_arrow_schema_get_pointer(self.reference)
  end
end
