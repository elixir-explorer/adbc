defmodule Adbc.Connection do
  @moduledoc """
  Documentation for `Adbc.Connection`.

  Connection are modelled as processes. They require
  an `Adbc.Database` to be started.
  """

  @type t :: GenServer.server()
  @type result_set :: Adbc.Result.t()

  use GenServer
  import Adbc.Helper, only: [error_to_exception: 1]

  @doc """
  Starts a connection process.

  ## Options

    * `:database` (required) - the database process to connect to

    * `:process_options` - the options to be given to the underlying
      process. See `GenServer.start_link/3` for all options

  ## Examples

      Adbc.Connection.start_link(
        database: MyApp.DB,
        process_options: [name: MyApp.Conn]
      )

  In your supervision tree it would be started like this:

      children = [
        {Adbc.Connection,
         database: MyApp.DB,
         process_options: [name: MyApp.Conn]}
      ]

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
  Runs the given `query` with `params`.
  """
  @spec query(t(), binary, [term]) :: {:ok, result_set} | {:error, Exception.t()}
  def query(conn, query, params \\ []) when is_binary(query) and is_list(params) do
    stream_lock(conn, {:query, query, params}, &stream_results/2)
  end

  @doc """
  Same as `query/3` but raises an exception on error.
  """
  @spec query!(t(), binary, [term]) :: result_set
  def query!(conn, query, params \\ []) when is_binary(query) and is_list(params) do
    case query(conn, query, params) do
      {:ok, result} -> result
      {:error, reason} -> raise reason
    end
  end

  @doc """
  Runs the given `query` with `params` and
  pass the ArrowStream pointer to the given function.

  The pointer will point to a valid ArrowStream through
  the duration of the function. The function may call
  native code that consumes the ArrowStream accordingly.
  """
  def query_pointer(conn, query, params \\ [], fun)
      when is_binary(query) and is_list(params) and is_function(fun) do
    stream_lock(conn, {:query, query, params}, fn stream_ref, rows_affected ->
      {:ok, fun.(Adbc.Nif.adbc_arrow_array_stream_get_pointer(stream_ref), rows_affected)}
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
    stream_lock(conn, {:adbc_connection_get_info, [info_codes]}, &stream_results/2)
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

    stream_lock(conn, {:adbc_connection_get_objects, args}, &stream_results/2)
  end

  @doc """
  Gets the underlying driver of a connection process.

  ## Examples

      ADBC.Connection.get_driver(conn)
      #=> {:ok, :sqlite}
  """
  @spec get_driver(t()) :: {:ok, atom() | String.t()} | :error
  def get_driver(conn) do
    with pid when pid != nil <- GenServer.whereis(conn),
         {:dictionary, dictionary} <- Process.info(pid, :dictionary),
         {:adbc_driver, module} <- List.keyfind(dictionary, :adbc_driver, 0),
         do: {:ok, module},
         else: (_ -> :error)
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
    stream_lock(conn, {:adbc_connection_get_table_types, []}, &stream_results/2)
  end

  defp stream_lock(conn, command, fun) do
    case GenServer.call(conn, {:stream_lock, command}, :infinity) do
      {:ok, conn, unlock_ref, stream_ref, rows_affected} ->
        try do
          fun.(stream_ref, normalize_rows(rows_affected))
        after
          GenServer.cast(conn, {:unlock, unlock_ref})
        end

      {:error, reason} ->
        {:error, error_to_exception(reason)}
    end
  end

  defp normalize_rows(-1), do: nil
  defp normalize_rows(rows) when is_integer(rows) and rows >= 0, do: rows

  defp stream_results(reference, num_rows), do: stream_results(reference, %{}, num_rows)

  defp stream_results(reference, acc, num_rows) do
    case Adbc.Nif.adbc_arrow_array_stream_next(reference) do
      {:ok, results, _done} ->
        acc = Map.merge(acc, Map.new(results), fn _k, v1, v2 -> v1 ++ v2 end)
        stream_results(reference, acc, num_rows)

      :end_of_series ->
        {:ok, %Adbc.Result{data: acc, num_rows: num_rows}}

      {:error, reason} ->
        {:error, error_to_exception(reason)}
    end
  end

  ## Callbacks

  @impl true
  def init({db, conn}) do
    case GenServer.call(db, {:initialize_connection, conn}, :infinity) do
      {:ok, driver} ->
        Process.flag(:trap_exit, true)
        Process.put(:adbc_driver, driver)
        {:ok, %{conn: conn, lock: :none, queue: :queue.new()}}

      {:error, reason} ->
        {:stop, error_to_exception(reason)}
    end
  end

  @impl true
  def handle_call({:stream_lock, command}, from, state) do
    state = update_in(state.queue, &:queue.in({:stream_lock, command, from}, &1))
    {:noreply, maybe_dequeue(state)}
  end

  @impl true
  def handle_cast({:unlock, ref}, %{lock: {ref, stream_ref}} = state) do
    # We could let the GC be the one collecting it but,
    # given this could be a large resource, we want to
    # release it as soon as possible.
    Adbc.Nif.adbc_arrow_array_stream_release(stream_ref)
    Process.demonitor(ref, [:flush])
    {:noreply, maybe_dequeue(%{state | lock: :none})}
  end

  @impl true
  def handle_info({:DOWN, ref, _, _, _}, %{lock: {ref, stream_ref}} = state) do
    Adbc.Nif.adbc_arrow_array_stream_release(stream_ref)
    {:noreply, maybe_dequeue(%{state | lock: :none})}
  end

  @impl true
  def handle_info({:EXIT, _db, reason}, state), do: {:stop, reason, state}
  def handle_info(_msg, state), do: {:noreply, state}

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

      {{:value, {:stream_lock, command, from}}, queue} ->
        {pid, _} = from

        case handle_command(command, state.conn) do
          {:ok, stream_ref, rows_affected} when is_reference(stream_ref) ->
            unlock_ref = Process.monitor(pid)
            GenServer.reply(from, {:ok, self(), unlock_ref, stream_ref, rows_affected})
            %{state | lock: {unlock_ref, stream_ref}, queue: queue}

          {:error, error} ->
            GenServer.reply(from, {:error, error})
            maybe_dequeue(%{state | queue: queue})
        end
    end
  end

  defp maybe_dequeue(state), do: state

  defp handle_command({:query, query, params}, conn) do
    with {:ok, stmt} <- Adbc.Nif.adbc_statement_new(conn),
         :ok <- Adbc.Nif.adbc_statement_set_sql_query(stmt, query),
         :ok <- maybe_bind(stmt, params) do
      Adbc.Nif.adbc_statement_execute_query(stmt)
    end
  end

  defp handle_command({name, args}, conn) do
    with {:ok, stream_ref} <- apply(Adbc.Nif, name, [conn | args]) do
      {:ok, stream_ref, -1}
    end
  end

  defp maybe_bind(_stmt, []), do: :ok
  defp maybe_bind(stmt, params), do: Adbc.Nif.adbc_statement_bind(stmt, params)
end
