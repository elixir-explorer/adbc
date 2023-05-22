defmodule Adbc.Statement do
  @typedoc """
  - **reference**: `reference`.

    The underlying erlang resource variable.

  """
  @type t :: %__MODULE__{
    reference: reference()
  }
  defstruct [:reference]
  alias __MODULE__, as: T

  @doc """
  Create a new statement for a given connection.
  """
  @doc group: :adbc_statement
  @spec new(Adbc.Connection.t()) :: {:ok, Adbc.Statement.t()} | Adbc.Error.adbc_error()
  def new(connection) do
    case Adbc.Nif.adbc_statement_new(connection.reference) do
    {:ok, ref} ->
      {:ok, %T{reference: ref}}

    {:error, {reason, code, sql_state}} ->
      {:error, {reason, code, sql_state}}
    end
  end

  @doc """
  Destroy a statement.

  ##### Positional Parameter

  - `self`: `Adbc.Statement.t()`

    The statement to release.

  """
  @doc group: :adbc_statement
  @spec release(Adbc.Statement.t()) :: :ok | Adbc.Error.adbc_error()
  def release(self = %T{}) do
    Adbc.Nif.adbc_statement_release(self.reference)
  end
end
