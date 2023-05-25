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

  def adbc_connection_get_table_schema(_self, _catalog, _db_schema, _table_name),
    do: :erlang.nif_error(:not_loaded)

  def adbc_connection_get_table_types(_self), do: :erlang.nif_error(:not_loaded)

  def adbc_connection_read_partition(_self, _serialized_partition, _serialized_length),
    do: :erlang.nif_error(:not_loaded)

  def adbc_connection_commit(_self), do: :erlang.nif_error(:not_loaded)

  def adbc_connection_rollback(_self), do: :erlang.nif_error(:not_loaded)

  def adbc_statement_new(_connection), do: :erlang.nif_error(:not_loaded)

  def adbc_statement_release(_statement), do: :erlang.nif_error(:not_loaded)

  def adbc_statement_execute_query(_statement), do: :erlang.nif_error(:not_loaded)

  def adbc_statement_prepare(_statement), do: :erlang.nif_error(:not_loaded)

  def adbc_statement_set_sql_query(_statement, _query), do: :erlang.nif_error(:not_loaded)

  def adbc_statement_set_substrait_plan(_statement, _plan, _length),
    do: :erlang.nif_error(:not_loaded)

  def adbc_statement_bind(_statement, _values, _schema), do: :erlang.nif_error(:not_loaded)

  def adbc_statement_bind_stream(_statement, _stream), do: :erlang.nif_error(:not_loaded)

  def adbc_statement_get_parameter_schema(_statement), do: :erlang.nif_error(:not_loaded)

  def adbc_get_all_function_pointers(), do: :erlang.nif_error(:not_loaded)
end
