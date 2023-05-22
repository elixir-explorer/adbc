defmodule Adbc.Connection do
  @moduledoc """
  Documentation for `Adbc.Connection`.
  """

  @typedoc """
  - **reference**: `reference`.

    The underlying erlang resource variable.

  """
  @type t :: %__MODULE__{
          reference: reference()
        }
  defstruct [:reference]
  alias __MODULE__, as: T
  alias Adbc.Database
  alias Adbc.ArrowArrayStream
  alias Adbc.ArrowSchema
  alias Adbc.Helper

  @doc """
  Allocate a new (but uninitialized) connection.
  """
  @doc group: :adbc_connection
  @spec new() :: {:ok, Adbc.Connection.t()} | Adbc.Error.adbc_error()
  def new do
    case Adbc.Nif.adbc_connection_new() do
      {:ok, ref} ->
        {:ok, %T{reference: ref}}

      {:error, {reason, code, sql_state}} ->
        {:error, {reason, code, sql_state}}
    end
  end

  @doc """
  Set an option.

  Options may be set before `Adbc.Connection.init/2`.  Some drivers may
  support setting options after initialization as well.
  """
  @doc group: :adbc_connection
  @spec set_option(Adbc.Connection.t(), String.t(), String.t()) :: :ok | Adbc.Error.adbc_error()
  def set_option(self = %T{}, key, value)
      when is_binary(key) and is_binary(value) do
    Adbc.Nif.adbc_connection_set_option(self.reference, key, value)
  end

  @doc """
  Finish setting options and initialize the connection.

  Some drivers may support setting options after initialization
  as well.
  """
  @doc group: :adbc_connection
  @spec init(Adbc.Connection.t(), %Database{}) :: :ok | Adbc.Error.adbc_error()
  def init(self = %T{}, database = %Database{}) do
    Adbc.Nif.adbc_connection_init(self.reference, database.reference)
  end

  @doc """
  Destroy this connection.
  """
  @doc group: :adbc_connection
  @spec release(Adbc.Connection.t()) :: :ok | Adbc.Error.adbc_error()
  def release(self = %T{}) do
    Adbc.Nif.adbc_connection_release(self.reference)
  end

  @doc """
  Get metadata about the database/driver.
  The result is an Arrow dataset with the following schema:

  ```markdown
  Field Name                  | Field Type
  ----------------------------|------------------------
  info_name                   | uint32 not null
  info_value                  | INFO_SCHEMA
  ```

  `INFO_SCHEMA` is a dense union with members:

  ```markdown
  Field Name (Type Code)      | Field Type
  ----------------------------|------------------------
  string_value (0)            | utf8
  bool_value (1)              | bool
  int64_value (2)             | int64
  int32_bitmask (3)           | int32
  string_list (4)             | list<utf8>
  int32_to_int32_list_map (5) | map<int32, list<int32>>
  ```

  Each metadatum is identified by an integer code.  The recognized
  codes are defined as constants.  Codes [0, 10_000) are reserved
  for ADBC usage.  Drivers/vendors will ignore requests for
  unrecognized codes (the row will be omitted from the result).

  ##### Positional Parameters

  - `self`: `%T{}`

    Current connection.

  - `info_codes`: `list(neg_integer())`

    A list of metadata codes to fetch, or `[]` to fetch all.

    Defaults to `[]`.
  """
  @doc group: :adbc_connection_metadata
  @spec get_info(Adbc.Connection.t(), list(neg_integer())) ::
          {:ok, Adbc.ArrowArrayStream.t()} | Adbc.Error.adbc_error()
  def get_info(self = %T{}, info_codes \\ []) when is_list(info_codes) do
    case Adbc.Nif.adbc_connection_get_info(self.reference, info_codes) do
      {:ok, array_stream_ref} ->
        {:ok, %ArrowArrayStream{reference: array_stream_ref}}

      {:error, {reason, code, sql_state}} ->
        {:error, {reason, code, sql_state}}
    end
  end

  @doc """
  Get a hierarchical view of all catalogs, database schemas, tables, and columns.

  The result is an Arrow dataset with the following schema:

  ```markdown
  | Field Name               | Field Type              |
  |--------------------------|-------------------------|
  | catalog_name             | utf8                    |
  | catalog_db_schemas       | list<DB_SCHEMA_SCHEMA>  |
  ```

  `DB_SCHEMA_SCHEMA` is a Struct with fields:

  ```markdown
  | Field Name               | Field Type              |
  |--------------------------|-------------------------|
  | db_schema_name           | utf8                    |
  | db_schema_tables         | list<TABLE_SCHEMA>      |
  ```

  `TABLE_SCHEMA` is a Struct with fields:

  ```markdown
  | Field Name               | Field Type              |
  |--------------------------|-------------------------|
  | table_name               | utf8 not null           |
  | table_type               | utf8 not null           |
  | table_columns            | list<COLUMN_SCHEMA>     |
  | table_constraints        | list<CONSTRAINT_SCHEMA> |
  ```

  `COLUMN_SCHEMA` is a Struct with fields:

  ```markdown
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
  ```

  1. The column's ordinal position in the table (starting from 1).
  2. Database-specific description of the column.
  3. Optional value.  Should be null if not supported by the driver.
     xdbc_ values are meant to provide JDBC/ODBC-compatible metadata
     in an agnostic manner.

  `CONSTRAINT_SCHEMA` is a Struct with fields:

  ```markdown
  | Field Name               | Field Type              | Comments |
  |--------------------------|-------------------------|----------|
  | constraint_name          | utf8                    |          |
  | constraint_type          | utf8 not null           | (1)      |
  | constraint_column_names  | list<utf8> not null     | (2)      |
  | constraint_column_usage  | list<USAGE_SCHEMA>      | (3)      |
  ```

  1. One of 'CHECK', 'FOREIGN KEY', 'PRIMARY KEY', or 'UNIQUE'.
  2. The columns on the current table that are constrained, in
     order.
  3. For FOREIGN KEY only, the referenced table and columns.

  `USAGE_SCHEMA` is a Struct with fields:

  ```markdown
  | Field Name               | Field Type              |
  |--------------------------|-------------------------|
  | fk_catalog               | utf8                    |
  | fk_db_schema             | utf8                    |
  | fk_table                 | utf8 not null           |
  | fk_column_name           | utf8 not null           |
  ```
  """
  @doc group: :adbc_connection_metadata
  @spec get_objects(Adbc.Connection.t(), non_neg_integer(), [
          {:catalog, String.t() | nil},
          {:db_schema, String.t() | nil},
          {:table_name, String.t() | nil},
          {:table_type, [String.t()] | nil},
          {:column_name, String.t() | nil}
        ]) :: {:ok, Adbc.ArrowArrayStream.t()} | Adbc.Error.adbc_error()
  def get_objects(self = %T{}, depth, opts \\ [])
      when is_integer(depth) and depth >= 0 do
    catalog = Helper.get_keyword!(opts, :catalog, :string, allow_nil: true)
    db_schema = Helper.get_keyword!(opts, :db_schema, :string, allow_nil: true)
    table_name = Helper.get_keyword!(opts, :table_name, :string, allow_nil: true)
    table_type = Helper.get_keyword!(opts, :table_type, [:string], allow_nil: true)
    column_name = Helper.get_keyword!(opts, :column_name, :string, allow_nil: true)

    case Adbc.Nif.adbc_connection_get_objects(
           self.reference,
           depth,
           catalog,
           db_schema,
           table_name,
           table_type,
           column_name
         ) do
      {:ok, array_stream_ref} ->
        {:ok, %ArrowArrayStream{reference: array_stream_ref}}

      {:error, {reason, code, sql_state}} ->
        {:error, {reason, code, sql_state}}
    end
  end

  @doc """
  Get the Arrow schema of a table.

  ##### Positional Parameters

  - `self`: `Adbc.Connection.t()`

    A valid `Adbc.Connection` struct.

  - `catalog`: `String.t()` | `nil`

    The catalog (or `nil` if not applicable).

  - `db_schema`: `String.t()` | `nil`

    The database schema (or `nil` if not applicable).

  - `table_name`: `String.t()`

    The table name.
  """
  @doc group: :adbc_connection_metadata
  @spec get_table_schema(Adbc.Connection.t(), String.t() | nil, String.t() | nil, String.t()) ::
          {:ok, Adbc.ArrowSchema.t()} | Adbc.Error.adbc_error()
  def get_table_schema(self = %T{}, catalog, db_schema, table_name)
      when (is_binary(catalog) or catalog == nil) and (is_binary(db_schema) or catalog == nil) and
             is_binary(table_name) do
    case Adbc.Nif.adbc_connection_get_table_schema(self.reference, catalog, db_schema, table_name) do
      {:ok, schema_ref} ->
        {:ok, %ArrowSchema{reference: schema_ref}}

      {:error, {reason, code, sql_state}} ->
        {:error, {reason, code, sql_state}}
    end
  end
end
