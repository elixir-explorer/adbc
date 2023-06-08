defmodule Adbc.Database do
  @moduledoc """
  Documentation for `Adbc.Database`.
  """

  @typedoc """
  - **reference**: `reference`.

    The underlying erlang resource variable.

  """
  @type t :: %__MODULE__{
          reference: reference()
        }
  defstruct [:reference]
  alias Adbc.Helper
  alias __MODULE__, as: T

  @doc """
  Allocate a new (but uninitialized) database.
  """
  @doc group: :adbc_database
  @spec new() :: {:ok, Adbc.Database.t()} | Adbc.Error.adbc_error()
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

  ##### Keyword Arguments

  - `version`, `String.t()`

  Specify which driver version to use.
  Valid only for official drivers at the moment.
  """
  @doc group: :adbc_database
  @spec set_option(Adbc.Database.t(), String.t(), String.t(), [{:version, String.t()}]) ::
          :ok | Adbc.Error.adbc_error() | {:error, String.t()}
  def set_option(self = %T{}, "driver", value, opts) when is_binary(value) do
    case value do
      "adbc_driver_" <> driver ->
        case Helper.shared_driver_path(driver, opts) do
          {:ok, path} ->
            Adbc.Nif.adbc_database_set_option(self.reference, "driver", path)

          {:error, reason} ->
            {:error, reason}
        end

      other ->
        Adbc.Nif.adbc_database_set_option(self.reference, "driver", other)
    end
  end

  @doc """
  Set an option.

  Options may be set before `Adbc.Database.init/1`.  Some drivers may
  support setting options after initialization as well.
  """
  @doc group: :adbc_database
  @spec set_option(Adbc.Database.t(), String.t(), String.t()) ::
          :ok | Adbc.Error.adbc_error() | {:error, String.t()}
  def set_option(self = %T{}, "driver", value) when is_binary(value) do
    case value do
      "adbc_driver_" <> driver ->
        case Helper.shared_driver_path(driver) do
          {:ok, path} ->
            Adbc.Nif.adbc_database_set_option(self.reference, "driver", path)

          {:error, reason} ->
            {:error, reason}
        end

      other ->
        Adbc.Nif.adbc_database_set_option(self.reference, "driver", other)
    end
  end

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
  @spec init(Adbc.Database.t()) :: :ok | Adbc.Error.adbc_error()
  def init(self = %T{}) do
    Adbc.Nif.adbc_database_init(self.reference)
  end

  @doc """
  Destroy this database. No connections may exist.
  """
  @doc group: :adbc_database
  @spec release(Adbc.Database.t()) :: :ok | Adbc.Error.adbc_error()
  def release(self = %T{}) do
    Adbc.Nif.adbc_database_release(self.reference)
  end
end
