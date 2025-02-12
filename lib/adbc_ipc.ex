defmodule Adbc.IPC do
  @moduledoc """
  Module for loading and saving Arrow IPC data.
  """

  import Adbc.Helper, only: [error_to_exception: 1]

  @doc """
  Load IPC from a file.
  """
  @spec load_file(Path.t()) :: term() | {:error, File.posix()}
  def load_file(filepath) do
    with {:ok, data} <- File.read(filepath) do
      load_ipc(data)
    end
  end

  @doc """
  Load from in-memory IPC data.
  """
  @spec load_ipc(binary) :: term() | {:error, Adbc.Error.t()}
  def load_ipc(data) do
    case Adbc.Nif.adbc_ipc_load_binary(data) do
      {:error, reason} ->
        {:error, error_to_exception(reason)}

      data ->
        data
    end
  end

  @doc """
  Dump Adbc.Result to in-memory IPC data.
  """
  @spec dump_ipc(list()) :: binary | {:error, Adbc.Error.t()}
  def dump_ipc(result) do
    case Adbc.Nif.adbc_ipc_dump_binary(result) do
      {:error, reason} ->
        {:error, error_to_exception(reason)}

      data ->
        data
    end
  end
end
