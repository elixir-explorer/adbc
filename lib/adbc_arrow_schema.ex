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
end
