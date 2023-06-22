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

    The statement to execute.
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

  ##### Positional Parameter

  - `self`: `Adbc.Statement.t()`

    A valid `Adbc.Statement.t()`
  """
  @doc group: :adbc_statement
  @spec prepare(Adbc.Statement.t()) :: :ok | Adbc.Error.adbc_error()
  def prepare(self = %T{}) do
    Adbc.Nif.adbc_statement_prepare(self.reference)
  end

  @doc """
  Set the SQL query to execute.

  The query can then be executed with `Adbc.Statement.execute/1`.  For
  queries expected to be executed repeatedly, `Adbc.Statement.prepare/1`
  the statement first.

  ##### Positional Parameter

  - `self`: `Adbc.Statement.t()`

    The statement.

  - `query`: `String.t()`

    The query to execute.

  """
  @doc group: :adbc_statement_sql
  @spec set_sql_query(Adbc.Statement.t(), String.t()) :: :ok | Adbc.Error.adbc_error()
  def set_sql_query(self = %T{}, query) when is_binary(query) do
    Adbc.Nif.adbc_statement_set_sql_query(self.reference, query)
  end

  @doc """
  Bind Arrow data. This can be used for bulk inserts or prepared statements.

  ##### Positional Parameter

  - `self`: `Adbc.Statement.t()`

    The statement.

  - `values`: `Adbc.ArrowArray.t()`

    The values to bind. The driver will call the release callback itself,
    although it may not do this until the statement is released.

  - `schema`: `Adbc.ArrowSchema.t()`

    The schema of the values to bind.

  """
  @doc group: :adbc_statement
  @spec bind(Adbc.Statement.t(), [integer() | float() | String.t() | nil | boolean()]) ::
          :ok | Adbc.Error.adbc_error()
  def bind(self = %T{}, values) when is_list(values) do
    Adbc.Nif.adbc_statement_bind(self.reference, values)
  end

  @doc """
  Bind Arrow data. This can be used for bulk inserts or prepared statements.

  ##### Positional Parameter

  - `self`: `Adbc.Statement.t()`

    The statement.

  - `stream`: `Adbc.ArrowArrayStream.t()`

    The values to bind. The driver will call the
    release callback itself, although it may not do this until the
    statement is released.

  """
  @doc group: :adbc_statement
  @spec bind_stream(Adbc.Statement.t(), Adbc.ArrowArrayStream.t()) ::
          :ok | Adbc.Error.adbc_error()
  def bind_stream(self = %T{}, stream = %ArrowArrayStream{}) do
    Adbc.Nif.adbc_statement_bind_stream(self.reference, stream.reference)
  end
end
