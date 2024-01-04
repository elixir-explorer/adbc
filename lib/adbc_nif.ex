defmodule Adbc.Nif do
  @moduledoc false

  @on_load :load_nif
  def load_nif do
    nif_file = ~c"#{:code.priv_dir(:adbc)}/adbc_nif"

    :ok =
      case :os.type() do
        {:win32, _} ->
          :dll_loader_helper_beam.add_dll_directory("#{:code.priv_dir(:adbc)}/bin")

        _ ->
          :ok
      end

    case :erlang.load_nif(nif_file, 0) do
      :ok -> :ok
      {:error, {:reload, _}} -> :ok
      {:error, reason} -> IO.puts("Failed to load nif: #{inspect(reason)}")
    end
  end

  def adbc_database_new, do: :erlang.nif_error(:not_loaded)

  def adbc_database_set_option(_self, _key, _value), do: :erlang.nif_error(:not_loaded)

  def adbc_database_init(_self), do: :erlang.nif_error(:not_loaded)

  def adbc_connection_new, do: :erlang.nif_error(:not_loaded)

  def adbc_connection_set_option(_self, _key, _value), do: :erlang.nif_error(:not_loaded)

  def adbc_connection_init(_self, _database), do: :erlang.nif_error(:not_loaded)

  def adbc_connection_get_info(_self, _info_codes), do: :erlang.nif_error(:not_loaded)

  def adbc_connection_get_objects(
        _self,
        _depth,
        _catalog,
        _db_schema,
        _table_name,
        _table_type,
        _column_name
      ),
      do: :erlang.nif_error(:not_loaded)

  def adbc_connection_get_table_types(_self), do: :erlang.nif_error(:not_loaded)

  def adbc_statement_new(_connection), do: :erlang.nif_error(:not_loaded)

  def adbc_statement_execute_query(_statement), do: :erlang.nif_error(:not_loaded)

  def adbc_statement_prepare(_statement), do: :erlang.nif_error(:not_loaded)

  def adbc_statement_set_sql_query(_statement, _query), do: :erlang.nif_error(:not_loaded)

  def adbc_statement_bind(_statement, _values), do: :erlang.nif_error(:not_loaded)

  def adbc_statement_bind_stream(_statement, _stream), do: :erlang.nif_error(:not_loaded)

  def adbc_arrow_array_stream_get_pointer(_arrow_array_stream), do: :erlang.nif_error(:not_loaded)

  def adbc_arrow_array_stream_next(_arrow_array_stream), do: :erlang.nif_error(:not_loaded)

  def adbc_arrow_array_stream_release(_arrow_array_stream), do: :erlang.nif_error(:not_loaded)
end
