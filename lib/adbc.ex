defmodule Adbc do
  @moduledoc """
  Bindings for Arrow Database Connectivity (ADBC).

  Adbc provides a standard database interface using the
  Apache Arrow format.

  ## Installation

  First, add `:adbc` as a dependency in your `mix.exs`:

      {:adbc, "~> 0.1"}

  Now, in your config/config.exs, configure the drivers you
  are going to use:

      config :adbc, :drivers, [:sqlite]

  If you are using a notebook or scripting, you can also use
  `Adbc.download_driver!/1` to dynamically download one.

  Then start the database and the relevant connection processes
  in your supervision tree:

      children = [
        {Adbc.Database,
         driver: :sqlite,
         process_options: [name: MyApp.DB]},
        {Adbc.Connection,
         database: MyApp.DB,
         process_options: [name: MyApp.Conn]}
      ]

      Supervisor.start_link(children, strategy: :one_for_one)

  In a notebook, the above would look like this:

      db = Kino.start_child!({Adbc.Database, driver: :sqlite})
      conn = Kino.start_child!({Adbc.Connection, database: db})

  And now you can make queries with:

      # For named connections
      {:ok, _} = Adbc.Connection.query(MyApp.Conn, "SELECT 123")

      # When using the conn PID directly
      {:ok, _} = Adbc.Connection.query(conn, "SELECT 123")

  ## Supported drivers

  Below we list all drivers supported out of the box. You may also
  give the full path to a driver in the `:driver` option to load other
  existing drivers.

  ### PostgreSQL

  The PostgreSQL driver provides access to any database that supports
  the PostgreSQL wire format. It is implemented as a wrapper around
  `libpq`.

  #### Examples

      {Adbc.Database, driver: :postgresql, uri: "postgresql://postgres@localhost"}

  You must pass the `:uri` option using Postgres'
  [connection URI](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING):

  ### Sqlite

  The SQLite driver provides access to SQLite databases.

  #### Examples

      {Adbc.Database, driver: :sqlite, uri: ":memory:"}
      {Adbc.Database, driver: :sqlite, uri: "./db/my.db"}
      {Adbc.Database, driver: :sqlite, uri: "file:/path/to/file/in/disk.db"}

  The `:uri` option should be, ":memory:", a filename or
  [URI filename](https://www.sqlite.org/c3ref/open.html#urifilenamesinsqlite3open).
  If omitted, it will default to an in-memory database, but
  one that is shared across all connections.

  ### Snowflake

  The Snowflake driver provides access to Snowflake Database Warehouses.

  #### Examples

      {Adbc.Database, driver: :snowflake, uri: "..."}

  The Snowflake URI should be of one of the following formats:

      user[:password]@account/database/schema[?param1=value1&paramN=valueN]
      user[:password]@account/database[?param1=value1&paramN=valueN]
      user[:password]@host:port/database/schema?account=user_account[&param1=value1&paramN=valueN]
      host:port/database/schema?account=user_account[&param1=value1&paramN=valueN]

  The first two are the most recommended formats. The schema, database and parameters are optional.
  See [Account identifiers](https://docs.snowflake.com/en/user-guide/admin-account-identifier) for more information.
  """

  @doc """
  Downloads a driver.

  See `Adbc` module doc for all supports drivers.
  It returns `:ok` or `{:error, binary}`.
  """
  @spec download_driver(atom, keyword) :: :ok | {:error, binary}
  def download_driver(driver, opts \\ []) when is_atom(driver) and is_list(opts) do
    Adbc.Driver.download(driver, opts)
  end

  @doc """
  Downloads a driver and raises in case of errors.

  See `Adbc` module doc for all supports drivers.
  It returns `:ok`.
  """
  @spec download_driver!(atom, keyword) :: :ok
  def download_driver!(driver, opts \\ []) when is_atom(driver) and is_list(opts) do
    case download_driver(driver, opts) do
      :ok -> :ok
      {:error, reason} -> raise "could not download Adbc driver for #{driver}: " <> reason
    end
  end
end

for driver <- Application.compile_env(:adbc, :drivers, []) do
  Adbc.download_driver!(driver)
end
