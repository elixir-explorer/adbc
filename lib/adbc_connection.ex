defmodule Adbc.Connection do
  @moduledoc """
  Documentation for `Adbc.Connection`.
  """

  defstruct [:reference]
  alias __MODULE__, as: T
  alias Adbc.Database
  alias Adbc.ArrayStream

  @doc """
  Allocate a new (but uninitialized) connection.
  """
  @doc group: :adbc_connection
  @spec new() :: {:ok, %T{}} | Adbc.Error.adbc_error()
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

  Options may be set before `Adbc.Connection.init/1`.  Some drivers may
  support setting options after initialization as well.
  """
  @doc group: :adbc_connection
  @spec set_option(%T{}, String.t(), String.t()) :: :ok | Adbc.Error.adbc_error()
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
  @spec init(%T{}, %Database{}) :: :ok | Adbc.Error.adbc_error()
  def init(self = %T{}, database = %Database{}) do
    Adbc.Nif.adbc_connection_init(self.reference, database.reference)
  end

  @doc """
  Destroy this connection.
  """
  @doc group: :adbc_connection
  @spec release(%T{}) :: :ok | Adbc.Error.adbc_error()
  def release(self = %T{}) do
    Adbc.Nif.adbc_connection_release(self.reference)
  end

  @doc """
  Get metadata about the database/driver.
  The result is an Arrow dataset with the following schema:

  ```
  Field Name                  | Field Type
  ----------------------------|------------------------
  info_name                   | uint32 not null
  info_value                  | INFO_SCHEMA
  ```

  `INFO_SCHEMA` is a dense union with members:

  ```
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
  @spec get_info(%T{}, list(neg_integer())) :: {:ok, %ArrayStream{}} | Adbc.Error.adbc_error()
  def get_info(self = %T{}, info_codes \\ []) when is_list(info_codes) do
    case Adbc.Nif.adbc_connection_get_info(self.reference, info_codes) do
      {:ok, array_stream_ref} ->
        {:ok, %ArrayStream{reference: array_stream_ref}}

      {:error, {reason, code, sql_state}} ->
        {:error, {reason, code, sql_state}}
    end
  end
end
