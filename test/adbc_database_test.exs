defmodule Adbc.DatabaseTest do
  use ExUnit.Case
  doctest Adbc.Database
  alias Adbc.Database

  describe "start_link" do
    test "starts a process" do
      assert {:ok, _} = Database.start_link(driver: :sqlite)
    end

    test "accepts process options" do
      assert {:ok, pid} =
               Database.start_link(driver: :sqlite, process_options: [name: :who_knows_db])

      assert Process.whereis(:who_knows_db) == pid
    end

    test "accepts driver from path" do
      {:ok, path} = Adbc.Driver.so_path(:sqlite)
      assert {:ok, pid} = Database.start_link(driver: path)
      assert Process.alive?(pid)
    end

    test "errors with invalid driver" do
      assert {:error, %ArgumentError{} = error} = Database.start_link(driver: :who_knows)
      assert Exception.message(error) =~ "unknown driver :who_knows"
    end

    test "errors with invalid option" do
      assert {:error, %Adbc.Error{} = error} =
               Database.start_link(driver: :sqlite, who_knows: 123)

      assert Exception.message(error) == "[SQLite] Unknown database option who_knows=123"
    end
  end

  describe "load custom driver" do
    @describetag :duckdb

    setup do
      {:ok, zip_data} =
        Adbc.Helper.download(
          "https://github.com/duckdb/duckdb/releases/download/v0.8.1/libduckdb-linux-amd64.zip",
          false
        )

      cache_path = Path.join(File.cwd!(), "libduckdb-linux-amd64.zip")
      File.write!(cache_path, zip_data)

      adbc_so_priv_dir =
        case :os.type() do
          {:win32, _} -> Application.app_dir(:adbc, "priv/bin")
          _ -> Application.app_dir(:adbc, "priv/lib")
        end

      cache_path = String.to_charlist(cache_path)
      {:ok, zip_handle} = :zip.zip_open(cache_path, [:memory])
      {:ok, zip_files} = :zip.table(cache_path)

      for {:zip_file, filename, _, _, _, _} <- zip_files,
          Path.extname(filename) == ".so" do
        {:ok, {filename, file_data}} = :zip.zip_get(filename, zip_handle)

        filename = to_string(filename)

        filepath = Path.join(adbc_so_priv_dir, filename)
        File.write!(filepath, file_data)
      end

      :ok = :zip.zip_close(zip_handle)

      %{path: Path.join(adbc_so_priv_dir, "libduckdb.so"), entrypoint: "duckdb_adbc_init"}
    end

    test "load duckdb", %{path: path, entrypoint: entrypoint} do
      assert {:ok, _} = Database.start_link(driver: path, entrypoint: entrypoint)
    end

    test "load duckdb with invalid entrypoint", %{path: path} do
      assert {:error, %Adbc.Error{} = error} = Database.start_link(driver: path, entrypoint: "entrypoint")

      assert String.starts_with?(Exception.message(error), "dlsym(entrypoint) failed:")
    end
  end
end
