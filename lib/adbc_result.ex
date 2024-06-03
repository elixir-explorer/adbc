defmodule Adbc.Result do
  @moduledoc """
  A struct returned as result from queries.

  It has two fields:

    * `:data` - a list of `Adbc.Column`

    * `:num_rows` - the number of rows returned, if returned
      by the database
  """
  defstruct [:num_rows, :data]

  @type t :: %Adbc.Result{
          num_rows: non_neg_integer() | nil,
          data: [%Adbc.Column{}]
        }
  @doc """
  Returns a map of columns as a result.
  """
  def to_map(result = %Adbc.Result{}) do
    Map.new(list_view_to_list(result).data, fn %Adbc.Column{name: name, type: type, data: data} ->
      case type do
        :list -> {name, Enum.map(data, &list_to_map/1)}
        _ -> {name, data}
      end
    end)
  end

  @doc """
  Convert any list view in the result set to normal lists.
  """
  @spec list_view_to_list(%Adbc.Result{}) :: %Adbc.Result{}
  def list_view_to_list(result = %Adbc.Result{data: data}) when is_list(data) do
    %{result | data: Enum.map(data, &Adbc.Column.list_view_to_list/1)}
  end

  defp list_to_map(%Adbc.Column{name: name, type: type, data: data}) do
    case type do
      :list ->
        list = Enum.map(data, &list_to_map/1)

        if name == "item" do
          list
        else
          {name, list}
        end

      :struct ->
        Enum.map(data, &list_to_map/1)

      _ ->
        if name == "item" do
          data
        else
          {name, data}
        end
    end
  end
end
