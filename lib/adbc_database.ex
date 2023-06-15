defmodule Adbc.Database do
  @moduledoc """
  Documentation for `Adbc.Database`.
  """

  use GenServer

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

  @doc """
  TODO.
  """
  def connection(database, timeout \\ 5000) do
    case GenServer.call(database, :connection, timeout) do
      {:ok, connection} ->
        {:ok, %Adbc.Connection{reference: connection}}

      {:error, reason} ->
        {:error, error_to_exception(reason)}
    end
  end

  ## Callbacks

  @impl true
  def init(db) do
    Process.flag(:trap_exit, true)
    {:ok, db}
  end

  @impl true
  def handle_call(:connection, _from, db) do
    # TODO: Accept options
    # TODO: Put connection behind a process
    result =
      with {:ok, ref} <- Adbc.Nif.adbc_connection_new(),
           :ok <- Adbc.Nif.adbc_connection_init(ref, db),
           do: {:ok, ref}

    {:reply, result, db}
  end

  @impl true
  def terminate(_, db) do
    Adbc.Nif.adbc_connection_release(db)
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

  defp error_to_exception(string) when is_binary(string) do
    ArgumentError.exception(string)
  end

  defp error_to_exception(list) when is_list(list) do
    ArgumentError.exception(List.to_string(list))
  end

  defp error_to_exception({:adbc_error, message, vendor_code, state}) do
    Adbc.Error.exception(message: message, vendor_code: vendor_code, state: state)
  end
end
