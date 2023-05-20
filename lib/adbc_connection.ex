defmodule Adbc.Connection do
  @moduledoc """
  Documentation for `Adbc.Connection`.
  """

  defstruct [:reference]
  alias __MODULE__, as: T
  alias Adbc.Database

  @doc """
  Allocate a new (but uninitialized) connection.
  """
  @doc group: :adbc_connection
  @spec new() :: {:ok, %T{}} | Adbc.Error.adbc_error()
  def new do
    case Adbc.Nif.adbc_connection_new() do
      {:ok, ref} ->
        {:ok, %T{reference: ref}}
      {:error, {reason, code, sql_state}} ->
        {:error, {reason, code, sql_state}}
    end
  end

  @doc """
  Set an option.

  Options may be set before `Adbc.Connection.init/1`.  Some drivers may
  support setting options after initialization as well.
  """
  @doc group: :adbc_connection
  @spec set_option(%T{}, String.t(), String.t()) :: :ok | Adbc.Error.adbc_error()
  def set_option(self=%T{}, key, value)
  when is_binary(key) and is_binary(value) do
    Adbc.Nif.adbc_connection_set_option(self.reference, key, value)
  end

  @doc """
  Finish setting options and initialize the connection.

  Some drivers may support setting options after initialization
  as well.
  """
  @doc group: :adbc_connection
  @spec init(%T{}, %Database{}) :: :ok | Adbc.Error.adbc_error()
  def init(self=%T{}, database=%Database{}) do
    Adbc.Nif.adbc_connection_init(self.reference, database.reference)
  end

  @doc """
  Destroy this connection.
  """
  @doc group: :adbc_connection
  @spec release(%T{}) :: :ok | Adbc.Error.adbc_error()
  def release(self=%T{}) do
    Adbc.Nif.adbc_connection_release(self.reference)
  end
end
