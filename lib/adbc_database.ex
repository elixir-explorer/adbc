defmodule Adbc.Database do
  @moduledoc """
  Documentation for `Adbc.Database`.
  """

  # TODO: Allow options to be set on the database after it has been initialized

  use GenServer
  import Adbc.Helper, only: [error_to_exception: 1]

  @doc """
  TODO.
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
      GenServer.start_link(__MODULE__, ref, process_options)
    else
      {:error, reason} -> {:error, error_to_exception(reason)}
    end
  end

  ## Callbacks

  @impl true
  def init(db) do
    Process.flag(:trap_exit, true)
    {:ok, db}
  end

  @impl true
  def handle_call({:initialize_connection, conn_ref}, {pid, _}, db) do
    case Adbc.Nif.adbc_connection_init(conn_ref, db) do
      :ok ->
        Process.link(pid)
        {:reply, :ok, db}

      {:error, reason} ->
        {:reply, {:error, reason}, db}
    end
  end

  @impl true
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  @impl true
  def terminate(_, db) do
    Adbc.Nif.adbc_database_release(db)
  end

  defp init_driver(ref, driver) do
    case Adbc.Driver.driver_filepath(driver) do
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
