defmodule Adbc.Result do
  defstruct [:num_rows, :data]

  @type t :: %Adbc.Result{
          num_rows: non_neg_integer() | nil,
          data: %{optional(binary) => list(term)}
        }
end
