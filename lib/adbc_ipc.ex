defmodule Adbc.IPC do
  @moduledoc """
  Module for loading and saving Arrow IPC data.
  """

  import Adbc.Helper, only: [error_to_exception: 1]
  alias Adbc.ArrayStream

  @doc """
  Return IPC endianness.
  """
  @spec endianness() :: :big | :little
  def endianness do
    Adbc.Nif.adbc_ipc_system_endianness()
  end

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

      stream_ref when is_reference(stream_ref) ->
        stream_ref
        |> ArrayStream.stream_results(nil)
        |> tap(fn _ -> Adbc.Nif.adbc_arrow_array_stream_release(stream_ref) end)
    end
  end

  @doc """
  Dump Adbc.Result to in-memory IPC data.
  """
  @spec dump_ipc(list(Adbc.Column.t())) :: binary | {:error, Adbc.Error.t()}
  def dump_ipc(columns) when is_list(columns) do
    case Adbc.Nif.adbc_ipc_dump_binary(columns) do
      {:error, reason} ->
        {:error, error_to_exception(reason)}

      data ->
        data
    end
  end
end
