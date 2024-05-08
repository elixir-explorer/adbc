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
  def to_map(r = %Adbc.Result{data: data}) do
    %Adbc.Result{
      r
      | data:
          Map.new(
            Enum.map(data, fn %Adbc.Column{name: name, type: type, data: data} ->
              case type do
                :list ->
                  {name, Enum.map(data, &list_to_map/1)}

                _ ->
                  {name, data}
              end
            end)
          )
    }
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

      _ ->
        if name == "item" do
          data
        else
          {name, data}
        end
    end
  end
end
