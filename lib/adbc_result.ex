defmodule Adbc.StreamResult do
  @moduledoc """
  Represents an unmaterialized Arrow stream from a query.

  This struct can only be used within the callback passed to
  `Adbc.Connection.query_pointer/4`. The stream can only be consumed
  **once** - after being passed to `bulk_insert/3` or other operations,
  it becomes invalid.

  It contains:

    * `:ref` - internal reference to the stream (do not use directly)
    * `:conn_ref` - internal connection reference (do not use directly)
    * `:pointer` - pointer to the ArrowArrayStream (integer memory address)
    * `:num_rows` - the number of rows affected by the query, may be `nil`
      for queries depending on the database driver
  """
  defstruct [:ref, :pointer, :num_rows]

  @type t :: %__MODULE__{
          ref: reference(),
          pointer: non_neg_integer(),
          num_rows: non_neg_integer() | nil
        }
end

defmodule Adbc.Result do
  @moduledoc """
  A struct returned as result from queries.

  It has two fields:

    * `:data` - a list of `Adbc.Column`. The `Adbc.Column` may
      not yet have been materialized

    * `:num_rows` - the number of rows returned, if returned
      by the database
  """
  defstruct [:num_rows, :data]

  @type t :: %Adbc.Result{
          num_rows: non_neg_integer() | nil,
          data: [Adbc.Column.t()]
        }

  @doc """
  `materialize/1` converts the result set's data from reference type to regular Elixir terms.
  """
  @spec materialize(%Adbc.Result{} | {:ok, %Adbc.Result{}} | {:error, String.t()}) ::
          %Adbc.Result{} | {:ok, %Adbc.Result{}} | {:error, String.t()}
  def materialize(%Adbc.Result{data: data} = result) when is_list(data) do
    %{result | data: Enum.map(data, &Adbc.Column.materialize/1)}
  end

  @doc """
  Returns a map of columns as a result.
  """
  def to_map(result = %Adbc.Result{}) do
    Map.new(result.data, fn %Adbc.Column{name: name} = column ->
      {name, column |> Adbc.Column.materialize() |> Adbc.Column.to_list()}
    end)
  end
end

defimpl Table.Reader, for: Adbc.Result do
  def init(result) do
    data =
      Enum.map(result.data, fn column ->
        column
        |> Adbc.Column.materialize()
        |> Adbc.Column.to_list()
      end)

    names = Enum.map(result.data, & &1.name)
    {:columns, %{columns: names, count: result.num_rows}, data}
  end
end
