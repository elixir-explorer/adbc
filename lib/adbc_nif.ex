defmodule Adbc.Nif do
  @moduledoc false

  @on_load :load_nif
  def load_nif do
    nif_file = ~c"#{:code.priv_dir(:adbc)}/adbc_nif"

    case :erlang.load_nif(nif_file, 0) do
      :ok -> :ok
      {:error, {:reload, _}} -> :ok
      {:error, reason} -> IO.puts("Failed to load nif: #{reason}")
    end
  end

  def adbc_database_new, do: :erlang.nif_error(:not_loaded)

  def adbc_database_set_option(_self, _key, _value), do: :erlang.nif_error(:not_loaded)

  def adbc_database_init(_self), do: :erlang.nif_error(:not_loaded)

  def adbc_database_release(_self), do: :erlang.nif_error(:not_loaded)

  def adbc_connection_new, do: :erlang.nif_error(:not_loaded)

  def adbc_connection_set_option(_self, _key, _value), do: :erlang.nif_error(:not_loaded)

  def adbc_connection_init(_self, _database), do: :erlang.nif_error(:not_loaded)

  def adbc_connection_release(_self), do: :erlang.nif_error(:not_loaded)

  def adbc_connection_get_info(_self, _info_codes), do: :erlang.nif_error(:not_loaded)
end
