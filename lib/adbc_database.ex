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

    with {:ok, ref} <- Adbc.Nif.adbc_database_new(),
         :ok <- init_driver(ref, driver),
         :ok <- init_options(ref, opts),
         :ok <- Adbc.Nif.adbc_database_init(ref) do
      GenServer.start_link(__MODULE__, {driver, ref}, process_options)
    else
      {:error, reason} -> {:error, error_to_exception(reason)}
    end
  end

  ## Callbacks

  @impl true
  def init({driver, db}) do
    Process.flag(:trap_exit, true)
    {:ok, {driver, db, %{}}}
  end

  @impl true
  def handle_call({:initialize_connection, conn_ref}, {pid, _}, {driver, db, conns}) do
    case Adbc.Nif.adbc_connection_init(conn_ref, db) do
      :ok ->
        Process.link(pid)
        conns = Map.put(conns, pid, true)
        {:reply, {:ok, driver}, {driver, db, conns}}

      {:error, reason} ->
        {:reply, {:error, reason}, {driver, db, conns}}
    end
  end

  @impl true
  def handle_info({:EXIT, pid, _reason}, {driver, db, conns}) do
    {:noreply, {driver, db, Map.delete(conns, pid)}}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, {_driver, _db, conns}) do
    for {pid, _} <- conns do
      ref = Process.monitor(pid)
      Process.exit(pid, :kill)

      receive do
        {:DOWN, ^ref, _, _, _} -> :ok
      end
    end

    :ok
  end

  defp init_driver(ref, driver) do
    case Adbc.Driver.so_path(driver) do
      {:ok, path} -> Adbc.Nif.adbc_database_set_option(ref, "driver", path)
      {:error, reason} -> {:error, reason}
    end
  end

  defp init_options(ref, opts) do
    Enum.reduce_while(opts, :ok, fn {key, value}, :ok ->
      case Adbc.Nif.adbc_database_set_option(ref, to_string(key), to_string(value)) do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end
end
