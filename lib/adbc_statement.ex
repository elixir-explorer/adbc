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
  alias Adbc.ArrowArrayStream

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

  @doc """
  Execute a statement and get the results.

  This invalidates any prior result sets.

  ##### Positional Parameter

  - `self`: `Adbc.Statement.t()`

    The statement to release.
  """
  @doc group: :adbc_statement
  @spec execute_query(Adbc.Statement.t()) ::
          {:ok, Adbc.ArrowArrayStream.t(), integer()} | Adbc.Error.adbc_error()
  def execute_query(self = %T{}) do
    case Adbc.Nif.adbc_statement_execute_query(self.reference) do
      {:ok, array_stream_ref, rows_affected} ->
        {:ok, %ArrowArrayStream{reference: array_stream_ref}, rows_affected}

      {:error, {reason, code, sql_state}} ->
        {:error, {reason, code, sql_state}}
    end
  end

  @doc """
  Turn this statement into a prepared statement to be
  executed multiple times.

  This invalidates any prior result sets.
  """
  @doc group: :adbc_statement
  @spec prepare(Adbc.Statement.t()) :: :ok | Adbc.Error.adbc_error()
  def prepare(self = %T{}) do
    Adbc.Nif.adbc_statement_prepare(self.reference)
  end
end
