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
end
