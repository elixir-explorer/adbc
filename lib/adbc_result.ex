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
          data: [%Adbc.Column{}]
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
