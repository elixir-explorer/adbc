defmodule Adbc.Database do
  @moduledoc """
  Documentation for `Adbc.Database`.
  """

  defstruct [:reference]
  alias __MODULE__, as: T

  @doc """
  Allocate a new (but uninitialized) database.
  """
  @doc group: :adbc_database
  @spec new() :: {:ok, %T{}} | Adbc.Error.adbc_error()
  def new do
    case Adbc.Nif.adbc_database_new() do
      {:ok, ref} ->
        {:ok, %T{reference: ref}}

      {:error, {reason, code, sql_state}} ->
        {:error, {reason, code, sql_state}}
    end
  end

  @doc """
  Set an option.

  Options may be set before `Adbc.Database.init/1`.  Some drivers may
  support setting options after initialization as well.
  """
  @doc group: :adbc_database
  @spec set_option(%T{}, String.t(), String.t()) :: :ok | Adbc.Error.adbc_error()
  def set_option(self = %T{}, key, value)
      when is_binary(key) and is_binary(value) do
    Adbc.Nif.adbc_database_set_option(self.reference, key, value)
  end

  @doc """
  Finish setting options and initialize the database.

  Some drivers may support setting options after initialization
  as well.
  """
  @doc group: :adbc_database
  @spec init(%T{}) :: :ok | Adbc.Error.adbc_error()
  def init(self = %T{}) do
    Adbc.Nif.adbc_database_init(self.reference)
  end

  @doc """
  Destroy this database. No connections may exist.
  """
  @doc group: :adbc_database
  @spec release(%T{}) :: :ok | Adbc.Error.adbc_error()
  def release(self = %T{}) do
    Adbc.Nif.adbc_database_release(self.reference)
  end
end
