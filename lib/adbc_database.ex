defmodule Adbc.Database do
  @moduledoc """
  Documentation for `Adbc.Database`.

  Databases are modelled as processes. They required
  a driver to be started.
  """

  use GenServer
  import Adbc.Helper, only: [error_to_exception: 1]

  @doc """
  Starts a database process.

  ## Options

    * `:driver` (required) - the driver to use for this database.
      It must be an atom (see `Adbc` module documentation for all
      built-in drivers) or a string representing the path to a driver

    * `:process_options` - the options to be given to the underlying
      process. See `GenServer.start_link/3` for all options

  All other options are given as database options to the underlying driver.

  ## Examples

      Adbc.Database.start_link(
        driver: :sqlite,
        process_options: [name: MyApp.DB]
      )

  In your supervision tree it would be started like this:

      children = [
        {Adbc.Database,
         driver: :sqlite,
         process_options: [name: MyApp.DB]},
      ]

  """
  def start_link(opts \\ []) do
    {driver, opts} = Keyword.pop(opts, :driver, nil)

    unless driver do
      raise ArgumentError, ":driver option must be specified"
    end

    {process_options, opts} = Keyword.pop(opts, :process_options, [])
    opts = Keyword.merge(driver_default_options(driver), opts)

    with {:ok, ref} <- Adbc.Nif.adbc_database_new(),
         :ok <- init_driver(ref, driver),
         :ok <- init_options(ref, opts),
         :ok <- Adbc.Nif.adbc_database_init(ref) do
      GenServer.start_link(__MODULE__, {driver, ref}, process_options)
    else
      {:error, reason} -> {:error, error_to_exception(reason)}
    end
  end

  @doc """
  Get a string type option of the database.
  """
  @spec get_string_option(pid(), atom() | String.t()) :: {:ok, String.t()} | {:error, String.t()}
  def get_string_option(db, key) when is_pid(db) do
    case GenServer.call(db, {:get_option, :string, to_string(key)}) do
      {:ok, value} ->
        {:ok, value}

      {:error, reason} ->
        {:error, error_to_exception(reason)}
    end
  end

  @doc """
  Get a binary (bytes) type option of the database.
  """
  @spec get_binary_option(pid(), atom() | String.t()) :: {:ok, binary()} | {:error, String.t()}
  def get_binary_option(db, key) when is_pid(db) do
    case GenServer.call(db, {:get_option, :bytes, to_string(key)}) do
      {:ok, value} ->
        {:ok, value}

      {:error, reason} ->
        {:error, error_to_exception(reason)}
    end
  end

  @doc """
  Get an integer type option of the database.
  """
  @spec get_integer_option(pid(), atom() | String.t()) :: {:ok, integer()} | {:error, String.t()}
  def get_integer_option(db, key) when is_pid(db) do
    case GenServer.call(db, {:get_option, :int, to_string(key)}) do
      {:ok, value} ->
        {:ok, value}

      {:error, reason} ->
        {:error, error_to_exception(reason)}
    end
  end

  @doc """
  Get a float type option of the database.
  """
  @spec get_float_option(pid(), atom() | String.t()) :: {:ok, float()} | {:error, String.t()}
  def get_float_option(db, key) when is_pid(db) do
    case GenServer.call(db, {:get_option, :double, to_string(key)}) do
      {:ok, value} ->
        {:ok, value}

      {:error, reason} ->
        {:error, error_to_exception(reason)}
    end
  end

  @doc """
  Set option for the database.

  - If `value` is an atom or a string, then corresponding string option will be set.
  - If `value` is a `{:byte, binary()}`-tuple, then corresponding binary option will be set.
  - If `value` is an integer, then corresponding integer option will be set.
  - If `value` is a float, then corresponding float option will be set.
  """
  @spec set_option(
          pid(),
          atom() | String.t(),
          atom() | {:bytes, binary()} | String.t() | number()
        ) ::
          :ok | {:error, String.t()}
  def set_option(db, key, value) when is_pid(db) do
    case GenServer.call(db, {:set_option, to_string(key), value}) do
      :ok ->
        :ok

      {:error, reason} ->
        {:error, error_to_exception(reason)}
    end
  end

  @doc """
  Set a string type option for the database.
  """
  @spec set_string_option(pid(), atom() | String.t(), term()) ::
          :ok | {:error, String.t()}
  def set_string_option(db, key, value) when is_pid(db) do
    case GenServer.call(db, {:set_option, to_string(key), to_string(value)}) do
      :ok ->
        :ok

      {:error, reason} ->
        {:error, error_to_exception(reason)}
    end
  end

  @doc """
  Set a binary type option for the database.
  """
  @spec set_binary_option(pid(), atom() | String.t(), binary()) ::
          :ok | {:error, String.t()}
  def set_binary_option(db, key, value) when is_pid(db) and is_binary(value) do
    case GenServer.call(db, {:set_option, to_string(key), {:bytes, value}}) do
      :ok ->
        :ok

      {:error, reason} ->
        {:error, error_to_exception(reason)}
    end
  end

  @doc """
  Set an integer type option for the database.
  """
  @spec set_integer_option(pid(), atom() | String.t(), integer()) ::
          :ok | {:error, String.t()}
  def set_integer_option(db, key, value) when is_pid(db) and is_integer(value) do
    case GenServer.call(db, {:set_option, to_string(key), value}) do
      :ok ->
        :ok

      {:error, reason} ->
        {:error, error_to_exception(reason)}
    end
  end

  @doc """
  Set a float type option for the database.
  """
  @spec set_float_option(pid(), atom() | String.t(), float()) ::
          :ok | {:error, String.t()}
  def set_float_option(db, key, value) when is_pid(db) and is_float(value) do
    case GenServer.call(db, {:set_option, to_string(key), value}) do
      :ok ->
        :ok

      {:error, reason} ->
        {:error, error_to_exception(reason)}
    end
  end

  defp driver_default_options(:duckdb), do: [entrypoint: "duckdb_adbc_init"]
  defp driver_default_options(_), do: []

  ## Callbacks

  @impl true
  def init({driver, db}) do
    Process.flag(:trap_exit, true)
    {:ok, {driver, db}}
  end

  @impl true
  def handle_call({:initialize_connection, conn_ref}, {pid, _}, {driver, db}) do
    case Adbc.Nif.adbc_connection_init(conn_ref, db) do
      :ok ->
        Process.link(pid)
        {:reply, {:ok, driver}, {driver, db}}

      {:error, reason} ->
        {:reply, {:error, reason}, {driver, db}}
    end
  end

  def handle_call({:get_option, option_type, key}, _from, {driver, db}) do
    {:reply, Adbc.Helper.get_option(:database, option_type, db, key), {driver, db}}
  end

  def handle_call({:set_option, key, value}, _from, {driver, db}) do
    {:reply, Adbc.Helper.set_option(:database, db, key, value), {driver, db}}
  end

  @impl true
  def handle_info(_msg, state), do: {:noreply, state}

  defp init_driver(ref, driver) do
    case Adbc.Driver.so_path(driver) do
      {:ok, path} -> Adbc.Helper.set_option(:database, ref, "driver", path)
      {:error, reason} -> {:error, reason}
    end
  end

  defp init_options(ref, opts) do
    Enum.reduce_while(opts, :ok, fn {key, value}, :ok ->
      case Adbc.Helper.set_option(:database, ref, key, value) do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end
end
