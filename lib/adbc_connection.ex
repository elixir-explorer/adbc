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

  @doc """
  Get a string type option of the connection.
  """
  @spec get_option(pid(), atom() | String.t()) :: {:ok, String.t()} | {:error, String.t()}
  def get_option(conn, key) when is_pid(conn) do
    case GenServer.call(conn, {:get_option, to_string(key)}) do
      {:ok, value} ->
        {:ok, value}

      {:error, reason} ->
        {:error, error_to_exception(reason)}
    end
  end

  @doc """
  Get a bytes type option of the connection.
  """
  @spec get_option_bytes(pid(), atom() | String.t()) :: {:ok, binary()} | {:error, String.t()}
  def get_option_bytes(conn, key) when is_pid(conn) do
    case GenServer.call(conn, {:get_option, :bytes, to_string(key)}) do
      {:ok, value} ->
        {:ok, value}

      {:error, reason} ->
        {:error, error_to_exception(reason)}
    end
  end

  @doc """
  Get an int type option of the connection.
  """
  @spec get_option_int(pid(), atom() | String.t()) :: {:ok, integer()} | {:error, String.t()}
  def get_option_int(conn, key) when is_pid(conn) do
    case GenServer.call(conn, {:get_option, :int, to_string(key)}) do
      {:ok, value} ->
        {:ok, value}

      {:error, reason} ->
        {:error, error_to_exception(reason)}
    end
  end

  @doc """
  Get a double type option of the connection.
  """
  @spec get_option_double(pid(), atom() | String.t()) :: {:ok, float()} | {:error, String.t()}
  def get_option_double(conn, key) when is_pid(conn) do
    case GenServer.call(conn, {:get_option, :double, to_string(key)}) do
      {:ok, value} ->
        {:ok, value}

      {:error, reason} ->
        {:error, error_to_exception(reason)}
    end
  end

  @doc """
  Set a string option of the connection.
  """
  @spec set_option(pid(), atom() | String.t(), atom() | String.t() | number()) ::
          :ok | {:error, String.t()}
  def set_option(conn, key, value) when is_pid(conn) do
    case GenServer.call(conn, {:set_option, to_string(key), value}) do
      :ok ->
        :ok

      {:error, reason} ->
        {:error, error_to_exception(reason)}
    end
  end

  defp init_options(ref, opts) do
    Enum.reduce_while(opts, :ok, fn {key, value}, :ok ->
      case Adbc.Helper.set_option(:connection, ref, key, value) do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp init_statement_options(ref, opts) do
    Enum.reduce_while(opts, :ok, fn {key, value}, :ok ->
      case Adbc.Helper.set_option(:statement, ref, key, value) do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  @doc """
  Runs the given `query` with `params`.
  """
  @spec query(t(), binary | reference, [term]) :: {:ok, result_set} | {:error, Exception.t()}
  def query(conn, query, params \\ [])
      when (is_binary(query) or is_reference(query)) and is_list(params) do
    stream(conn, {:query, query, params}, &stream_results/2)
  end

  @doc """
  Same as `query/3` but raises an exception on error.
  """
  @spec query!(t(), binary | reference, [term]) :: result_set
  def query!(conn, query, params \\ [])
      when (is_binary(query) or is_reference(query)) and is_list(params) do
    case query(conn, query, params) do
      {:ok, result} -> result
      {:error, reason} -> raise reason
    end
  end

  @doc """
  Runs the given `query` with `params`.
  """
  @spec query_with_options(t(), binary | reference, [term], Keyword.t()) ::
          {:ok, result_set} | {:error, Exception.t()}
  def query_with_options(conn, query, params \\ [], statement_options)
      when (is_binary(query) or is_reference(query)) and is_list(params) and
             is_list(statement_options) do
    stream(conn, {:query, query, params, statement_options}, &stream_results/2)
  end

  @doc """
  Same as `query_with_options/4` but raises an exception on error.
  """
  @spec query_with_options!(t(), binary | reference, [term], Keyword.t()) :: result_set
  def query_with_options!(conn, query, params \\ [], statement_options)
      when (is_binary(query) or is_reference(query)) and is_list(params) and
             is_list(statement_options) do
    case query_with_options(conn, query, params, statement_options) do
      {:ok, result} -> result
      {:error, reason} -> raise reason
    end
  end

  @doc """
  Prepares the given `query`.
  """
  @spec prepare(t(), binary) :: {:ok, reference} | {:error, Exception.t()}
  def prepare(conn, query) when is_binary(query) do
    command(conn, {:prepare, query})
  end

  @doc """
  Runs the given `query` with `params` and
  pass the ArrowStream pointer to the given function.

  The pointer will point to a valid ArrowStream through
  the duration of the function. The function may call
  native code that consumes the ArrowStream accordingly.
  """
  def query_pointer(conn, query, params \\ [], fun)
      when (is_binary(query) or is_reference(query)) and is_list(params) and is_function(fun) do
    stream(conn, {:query, query, params}, fn stream_ref, rows_affected ->
      {:ok, fun.(Adbc.Nif.adbc_arrow_array_stream_get_pointer(stream_ref), rows_affected)}
    end)
  end

  @doc """
  Get metadata about the database/driver.

  The result is an Arrow dataset with the following schema:


  | Field Name                 |  Field Type    | Null Contstraint  |
  | -------------------------- | ---------------|------------------ |
  | `info_name`                |  `uint32`      | not null          |
  | `info_value`               |  `INFO_SCHEMA` |                   |

  `INFO_SCHEMA` is a dense union with members:

  | Field Name       | Type Code |  Field Type                   |
  | -----------------| --------- | ----------------------------- |
  | `string_value`              | 0 |  `utf8`                    |
  | `bool_value`                | 1 |  `bool`                    |
  | `int64_value`               | 2 |  `int64`                   |
  | `int32_bitmask`             | 3 |  `int32`                   |
  | `string_list`               | 4 |  `list<utf8>`              |
  | `int32_to_int32_list_map`   | 5 |  `map<int32, list<int32>>` |

  Each metadatum is identified by an integer code. The recognized
  codes are defined as constants. Codes [0, 10_000) are reserved
  for ADBC usage. Drivers/vendors will ignore requests for
  unrecognized codes (the row will be omitted from the result).
  """
  @spec get_info(t(), list(non_neg_integer())) ::
          {:ok, result_set} | {:error, Exception.t()}
  def get_info(conn, info_codes \\ []) when is_list(info_codes) do
    stream(conn, {:adbc_connection_get_info, [info_codes]}, &stream_results/2)
  end

  @doc """
  Get a hierarchical view of all catalogs, database schemas, tables, and columns.

  The result is an Arrow dataset with the following schema:

  | Field Name               | Field Type                |
  |--------------------------|---------------------------|
  | `catalog_name`           | `utf8`                    |
  | `catalog_db_schemas`     | `list<DB_SCHEMA_SCHEMA>`  |

  `DB_SCHEMA_SCHEMA` is a Struct with fields:

  | Field Name             | Field Type              |
  |------------------------|-------------------------|
  | `db_schema_name`       | `utf8`                  |
  | `db_schema_tables`     | `list<TABLE_SCHEMA>`    |

  `TABLE_SCHEMA` is a Struct with fields:

  | Field Name           | Field Type                | Null Contstraint   |
  |----------------------|---------------------------|--------------------|
  | `table_name`         | `utf8`                    | not null           |
  | `table_type`         | `utf8`                    | not null           |
  | `table_columns`      | `list<COLUMN_SCHEMA>`     |                    |
  | `table_constraints`  | `list<CONSTRAINT_SCHEMA>` |                    |

  `COLUMN_SCHEMA` is a Struct with fields:

  | Field Name               | Field Type  | Null Contstraint | Comments |
  |--------------------------|-------------|-----------       |----------|
  | `column_name`              | `utf8`    | not null         |          |
  | `ordinal_position`         | `int32`   |                  | (1)      |
  | `remarks`                  | `utf8`    |                  | (2)      |
  | `xdbc_data_type`           | `int16`   |                  | (3)      |
  | `xdbc_type_name`           | `utf8`    |                  | (3)      |
  | `xdbc_column_size`         | `int32`   |                  | (3)      |
  | `xdbc_decimal_digits`      | `int16`   |                  | (3)      |
  | `xdbc_num_prec_radix`      | `int16`   |                  | (3)      |
  | `xdbc_nullable`            | `int16`   |                  | (3)      |
  | `xdbc_column_def`          | `utf8`    |                  | (3)      |
  | `xdbc_sql_data_type`       | `int16`   |                  | (3)      |
  | `xdbc_datetime_sub`        | `int16`   |                  | (3)      |
  | `xdbc_char_octet_length`   | `int32`   |                  | (3)      |
  | `xdbc_is_nullable`         | `utf8`    |                  | (3)      |
  | `xdbc_scope_catalog`       | `utf8`    |                  | (3)      |
  | `xdbc_scope_schema`        | `utf8`    |                  | (3)      |
  | `xdbc_scope_table`         | `utf8`    |                  | (3)      |
  | `xdbc_is_autoincrement`    | `bool`    |                  | (3)      |
  | `xdbc_is_generatedcolumn`  | `bool`    |                  | (3)      |

  1. The column's ordinal position in the table (starting from 1).
  2. Database-specific description of the column.
  3. Optional value. Should be null if not supported by the driver.
     `xdbc_` values are meant to provide JDBC/ODBC-compatible metadata
     in an agnostic manner.

  `CONSTRAINT_SCHEMA` is a Struct with fields:

  | Field Name                | Field Type         | Null Contstraint | Comments |
  |---------------------------|--------------------|------------------|----------|
  | `constraint_name`         | `utf8`               |                  |          |
  | `constraint_type`         | `utf8`               | not null         | (1)      |
  | `constraint_column_names` | `list<utf8>`         | not null         | (2)      |
  | `constraint_column_usage` | `list<USAGE_SCHEMA>` |                  | (3)      |

  1. One of 'CHECK', 'FOREIGN KEY', 'PRIMARY KEY', or 'UNIQUE'.
  2. The columns on the current table that are constrained, in order.
  3. For FOREIGN KEY only, the referenced table and columns.

  `USAGE_SCHEMA` is a Struct with fields:

  | Field Name               | Field Type  | Null Contstraint |
  |--------------------------|-------------|------------------|
  | `fk_catalog`             | `utf8`        |                  |
  | `fk_db_schema`           | `utf8`        |                  |
  | `fk_table`               | `utf8`        | not null         |
  | `fk_column_name`         | `utf8`        | not null         |
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

    stream(conn, {:adbc_connection_get_objects, args}, &stream_results/2)
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

  | Field Name     | Field Type    | Null Contstraint |
  |----------------|---------------|------------------|
  | `table_type`   | `utf8`        | not null         |

  """
  @spec get_table_types(t) ::
          {:ok, result_set} | {:error, Exception.t()}
  def get_table_types(conn) do
    stream(conn, {:adbc_connection_get_table_types, []}, &stream_results/2)
  end

  defp command(conn, command) do
    case GenServer.call(conn, {:command, command}, :infinity) do
      {:ok, result} -> {:ok, result}
      {:error, reason} -> {:error, error_to_exception(reason)}
    end
  end

  defp stream(conn, command, fun) do
    case GenServer.call(conn, {:stream, command}, :infinity) do
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
        Process.put(:adbc_driver, driver)
        {:ok, %{conn: conn, lock: :none, queue: :queue.new()}}

      {:error, reason} ->
        {:stop, error_to_exception(reason)}
    end
  end

  @impl true
  def handle_call({:stream, command}, from, state) do
    state = update_in(state.queue, &:queue.in({:stream, command, from}, &1))
    {:noreply, maybe_dequeue(state)}
  end

  def handle_call({:command, command}, from, state) do
    state = update_in(state.queue, &:queue.in({:command, command, from}, &1))
    {:noreply, maybe_dequeue(state)}
  end

  def handle_call({:get_option, option_type, key}, _from, state = %{conn: conn}) do
    {:reply, Adbc.Helper.get_option(:connection, option_type, conn, key), state}
  end

  def handle_call({:set_option, key, value}, _from, state = %{conn: conn}) do
    {:reply, Adbc.Helper.set_option(:connection, conn, key, value), state}
  end

  @impl true
  def handle_cast({:unlock, ref}, %{lock: {ref, stream_ref}} = state) do
    # We could let the GC be the one release it but,
    # since a stream can be a large resource, we release
    # it now and let the GC free the remaining resources.
    Adbc.Nif.adbc_arrow_array_stream_release(stream_ref)
    Process.demonitor(ref, [:flush])
    {:noreply, maybe_dequeue(%{state | lock: :none})}
  end

  @impl true
  def handle_info({:DOWN, ref, _, _, _}, %{lock: {ref, stream_ref}} = state) do
    Adbc.Nif.adbc_arrow_array_stream_release(stream_ref)
    {:noreply, maybe_dequeue(%{state | lock: :none})}
  end

  ## Queue helpers

  defp maybe_dequeue(%{lock: :none, queue: queue} = state) do
    case :queue.out(queue) do
      {:empty, queue} ->
        %{state | queue: queue}

      {{:value, {:command, command, from}}, queue} ->
        result = handle_command(command, state.conn)
        GenServer.reply(from, result)
        maybe_dequeue(%{state | queue: queue})

      {{:value, {:stream, command, from}}, queue} ->
        {pid, _} = from

        case handle_stream(command, state.conn) do
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

  defp handle_command({:prepare, query}, conn) do
    with {:ok, stmt} <- create_statement(conn, query),
         :ok <- Adbc.Nif.adbc_statement_prepare(stmt) do
      {:ok, stmt}
    end
  end

  defp handle_stream({:query, query_or_prepared, params}, conn) do
    with {:ok, stmt} <- ensure_statement(conn, query_or_prepared),
         :ok <- maybe_bind(stmt, params) do
      Adbc.Nif.adbc_statement_execute_query(stmt)
    end
  end

  defp handle_stream({:query, query_or_prepared, params, statement_options}, conn) do
    with {:ok, stmt} <- ensure_statement(conn, query_or_prepared, statement_options),
         :ok <- maybe_bind(stmt, params) do
      Adbc.Nif.adbc_statement_execute_query(stmt)
    end
  end

  defp handle_stream({name, args}, conn) do
    with {:ok, stream_ref} <- apply(Adbc.Nif, name, [conn | args]) do
      {:ok, stream_ref, -1}
    end
  end

  defp ensure_statement(conn, query, statement_options \\ [])

  defp ensure_statement(conn, query, statement_options)
       when is_binary(query) and is_list(statement_options),
       do: create_statement(conn, query, statement_options)

  defp ensure_statement(_conn, prepared, _statement_options) when is_reference(prepared),
    do: {:ok, prepared}

  defp create_statement(conn, query, statement_options \\ []) when is_list(statement_options) do
    with {:ok, stmt} <- Adbc.Nif.adbc_statement_new(conn),
         :ok <- Adbc.Nif.adbc_statement_set_sql_query(stmt, query),
         :ok <- init_statement_options(stmt, statement_options) do
      {:ok, stmt}
    end
  end

  defp maybe_bind(_stmt, []), do: :ok
  defp maybe_bind(stmt, params), do: Adbc.Nif.adbc_statement_bind(stmt, params)
end
