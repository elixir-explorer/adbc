defmodule Adbc.Connection do
  @moduledoc """
  Documentation for `Adbc.Connection`.
  """

  # TODO: Add prepared queries

  @type t :: GenServer.server()
  @type result_set :: map

  use GenServer
  import Adbc.Helper, only: [error_to_exception: 1]

  @doc """
  TODO.
  """
  def start_link(opts) do
    {db, opts} = Keyword.pop(opts, :database, nil)

    unless db do
      raise ArgumentError, ":database option must be specified"
    end

    {process_options, opts} = Keyword.pop(opts, :process_options, [])

    with {:ok, conn} <- Adbc.Nif.adbc_connection_new(),
         :ok <- init_options(conn, opts) do
      GenServer.start_link(__MODULE__, {db, conn}, process_options)
    else
      {:error, reason} -> {:error, error_to_exception(reason)}
    end
  end

  defp init_options(ref, opts) do
    Enum.reduce_while(opts, :ok, fn {key, value}, :ok ->
      case Adbc.Nif.adbc_connection_set_option(ref, to_string(key), to_string(value)) do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  @doc """
  TODO.
  """
  def query(conn, query, params \\ []) when is_binary(query) and is_list(params) do
    stream_lock(conn, {:query, query, params}, &stream_results/1)
  end

  @doc """
  TODO.
  """
  def query_pointer(conn, query, params \\ [], fun)
      when is_binary(query) and is_list(params) and is_function(fun) do
    stream_lock(conn, {:query, query, params}, fn stream_ref ->
      fun.(Adbc.Nif.adbc_arrow_array_stream_get_pointer(stream_ref))
    end)
  end

  @doc """
  Get metadata about the database/driver.

  The result is an Arrow dataset with the following schema:

  Field Name                  | Field Type
  ----------------------------|------------------------
  info_name                   | uint32 not null
  info_value                  | INFO_SCHEMA

  `INFO_SCHEMA` is a dense union with members:

  Field Name (Type Code)      | Field Type
  ----------------------------|------------------------
  string_value (0)            | utf8
  bool_value (1)              | bool
  int64_value (2)             | int64
  int32_bitmask (3)           | int32
  string_list (4)             | list<utf8>
  int32_to_int32_list_map (5) | map<int32, list<int32>>

  Each metadatum is identified by an integer code. The recognized
  codes are defined as constants. Codes [0, 10_000) are reserved
  for ADBC usage. Drivers/vendors will ignore requests for
  unrecognized codes (the row will be omitted from the result).
  """
  @spec get_info(t(), list(non_neg_integer())) ::
          {:ok, result_set} | {:error, Exception.t()}
  def get_info(conn, info_codes \\ []) when is_list(info_codes) do
    stream_lock(conn, {:adbc_connection_get_info, [info_codes]}, &stream_results/1)
  end

  @doc """
  Get a hierarchical view of all catalogs, database schemas, tables, and columns.

  The result is an Arrow dataset with the following schema:

  | Field Name               | Field Type              |
  |--------------------------|-------------------------|
  | catalog_name             | utf8                    |
  | catalog_db_schemas       | list<DB_SCHEMA_SCHEMA>  |

  `DB_SCHEMA_SCHEMA` is a Struct with fields:

  | Field Name               | Field Type              |
  |--------------------------|-------------------------|
  | db_schema_name           | utf8                    |
  | db_schema_tables         | list<TABLE_SCHEMA>      |

  `TABLE_SCHEMA` is a Struct with fields:

  | Field Name               | Field Type              |
  |--------------------------|-------------------------|
  | table_name               | utf8 not null           |
  | table_type               | utf8 not null           |
  | table_columns            | list<COLUMN_SCHEMA>     |
  | table_constraints        | list<CONSTRAINT_SCHEMA> |

  `COLUMN_SCHEMA` is a Struct with fields:

  | Field Name               | Field Type              | Comments |
  |--------------------------|-------------------------|----------|
  | column_name              | utf8 not null           |          |
  | ordinal_position         | int32                   | (1)      |
  | remarks                  | utf8                    | (2)      |
  | xdbc_data_type           | int16                   | (3)      |
  | xdbc_type_name           | utf8                    | (3)      |
  | xdbc_column_size         | int32                   | (3)      |
  | xdbc_decimal_digits      | int16                   | (3)      |
  | xdbc_num_prec_radix      | int16                   | (3)      |
  | xdbc_nullable            | int16                   | (3)      |
  | xdbc_column_def          | utf8                    | (3)      |
  | xdbc_sql_data_type       | int16                   | (3)      |
  | xdbc_datetime_sub        | int16                   | (3)      |
  | xdbc_char_octet_length   | int32                   | (3)      |
  | xdbc_is_nullable         | utf8                    | (3)      |
  | xdbc_scope_catalog       | utf8                    | (3)      |
  | xdbc_scope_schema        | utf8                    | (3)      |
  | xdbc_scope_table         | utf8                    | (3)      |
  | xdbc_is_autoincrement    | bool                    | (3)      |
  | xdbc_is_generatedcolumn  | bool                    | (3)      |

  1. The column's ordinal position in the table (starting from 1).
  2. Database-specific description of the column.
  3. Optional value. Should be null if not supported by the driver.
     xdbc_ values are meant to provide JDBC/ODBC-compatible metadata
     in an agnostic manner.

  `CONSTRAINT_SCHEMA` is a Struct with fields:

  | Field Name               | Field Type              | Comments |
  |--------------------------|-------------------------|----------|
  | constraint_name          | utf8                    |          |
  | constraint_type          | utf8 not null           | (1)      |
  | constraint_column_names  | list<utf8> not null     | (2)      |
  | constraint_column_usage  | list<USAGE_SCHEMA>      | (3)      |

  1. One of 'CHECK', 'FOREIGN KEY', 'PRIMARY KEY', or 'UNIQUE'.
  2. The columns on the current table that are constrained, in order.
  3. For FOREIGN KEY only, the referenced table and columns.

  `USAGE_SCHEMA` is a Struct with fields:

  | Field Name               | Field Type              |
  |--------------------------|-------------------------|
  | fk_catalog               | utf8                    |
  | fk_db_schema             | utf8                    |
  | fk_table                 | utf8 not null           |
  | fk_column_name           | utf8 not null           |
  """
  @spec get_objects(
          t(),
          non_neg_integer(),
          catalog: String.t(),
          db_schema: String.t(),
          table_name: String.t(),
          table_type: [String.t()],
          column_name: String.t()
        ) :: {:ok, result_set} | {:error, Exception.t()}
  def get_objects(conn, depth, opts \\ [])
      when is_integer(depth) and depth >= 0 do
    opts = Keyword.validate!(opts, [:catalog, :db_schema, :table_name, :table_type, :column_name])

    args = [
      depth,
      opts[:catalog],
      opts[:db_schema],
      opts[:table_name],
      opts[:table_type],
      opts[:column_name]
    ]

    stream_lock(conn, {:adbc_connection_get_objects, args}, &stream_results/1)
  end

  @doc """
  Get a list of table types in the database.

  The result is an Arrow dataset with the following schema:

  Field Name     | Field Type
  ---------------|--------------
  table_type     | utf8 not null
  """
  @spec get_table_types(t) ::
          {:ok, result_set} | {:error, Exception.t()}
  def get_table_types(conn) do
    stream_lock(conn, {:adbc_connection_get_table_types, []}, &stream_results/1)
  end

  defp stream_lock(conn, command, fun) do
    case GenServer.call(conn, {:lock, command}, :infinity) do
      {:ok, conn, unlock_ref, stream_ref} when is_reference(stream_ref) ->
        try do
          fun.(stream_ref)
        after
          # TODO: force release the arrow array stream on the server
          # Adbc.Nif.adbc_arrow_array_stream_release(stream_ref)
          GenServer.cast(conn, {:unlock, unlock_ref})
        end

      {:error, reason} ->
        {:error, error_to_exception(reason)}
    end
  end

  defp stream_results(reference) do
    stream_results(reference, %{})
  end

  defp stream_results(reference, acc) do
    case Adbc.Nif.adbc_arrow_array_stream_next(reference) do
      {:ok, results, done} ->
        acc = Map.merge(acc, Map.new(results), fn _k, v1, v2 -> v1 ++ v2 end)

        case done do
          0 -> stream_results(reference, acc)
          1 -> {:ok, acc}
        end

      {:error, reason} ->
        {:error, error_to_exception(reason)}
    end
  end

  ## Callbacks

  @impl true
  def init({db, conn}) do
    case GenServer.call(db, {:initialize_connection, conn}, :infinity) do
      :ok ->
        Process.flag(:trap_exit, true)
        {:ok, %{conn: conn, lock: :none, queue: :queue.new()}}

      {:error, reason} ->
        {:stop, error_to_exception(reason)}
    end
  end

  @impl true
  def handle_call({:queue, command}, from, state) do
    state = update_in(state.queue, &:queue.in({:queue, command, from}, &1))
    {:noreply, maybe_dequeue(state)}
  end

  @impl true
  def handle_call({:lock, command}, from, state) do
    state = update_in(state.queue, &:queue.in({:lock, command, from}, &1))
    {:noreply, maybe_dequeue(state)}
  end

  @impl true
  def handle_cast({:unlock, ref}, %{lock: ref} = state) do
    Process.demonitor(ref, [:flush])
    {:noreply, maybe_dequeue(%{state | lock: :none})}
  end

  @impl true
  def handle_info({:DOWN, ref, _, _, _}, %{lock: ref} = state) do
    {:noreply, maybe_dequeue(%{state | lock: :none})}
  end

  @impl true
  def handle_info({:EXIT, _db, reason}, state), do: {:stop, reason, state}
  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    Adbc.Nif.adbc_connection_release(state.conn)
    :ok
  end

  ## Queue helpers

  defp maybe_dequeue(%{lock: :none, queue: queue} = state) do
    case :queue.out(queue) do
      {:empty, queue} ->
        %{state | queue: queue}

      {{:value, {:queue, command, from}}, queue} ->
        {name, args} = command
        result = apply(Adbc.Nif, name, [state.conn | args])
        GenServer.reply(from, result)
        maybe_dequeue(%{state | queue: queue})

      {{:value, {:lock, command, from}}, queue} ->
        {pid, _} = from

        case handle_command(command, state.conn) do
          {:ok, something} ->
            unlock_ref = Process.monitor(pid)
            GenServer.reply(from, {:ok, self(), unlock_ref, something})
            %{state | lock: unlock_ref, queue: queue}

          {:error, error} ->
            # TODO: Test that error dequeues next
            GenServer.reply(from, {:error, error})
            maybe_dequeue(%{state | queue: queue})
        end
    end
  end

  defp maybe_dequeue(state), do: state

  defp handle_command({:query, query, params}, conn) do
    with {:ok, stmt} <- Adbc.Nif.adbc_statement_new(conn),
         :ok <- Adbc.Nif.adbc_statement_set_sql_query(stmt, query),
         :ok <- maybe_bind(stmt, params),
         {:ok, stream_ref, _rows_affected} <- Adbc.Nif.adbc_statement_execute_query(stmt) do
      {:ok, stream_ref}
    end
  end

  defp handle_command({name, args}, conn) do
    apply(Adbc.Nif, name, [conn | args])
  end

  defp maybe_bind(_stmt, []), do: :ok
  defp maybe_bind(stmt, params), do: Adbc.Nif.adbc_statement_bind(stmt, params)
end
