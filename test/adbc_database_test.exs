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
               Database.start_link(driver: :sqlite, process_options: [name: :who_knows])

      assert Process.whereis(:who_knows) == pid
    end

    test "errors with invalid driver" do
      assert {:error, %ArgumentError{} = error} = Database.start_link(driver: :who_knows)
      assert Exception.message(error) == "unknown driver :who_knows"
    end

    test "errors with invalid option" do
      assert {:error, %Adbc.Error{} = error} =
               Database.start_link(driver: :sqlite, who_knows: 123)

      assert Exception.message(error) == "[SQLite] Unknown database option who_knows=123"
    end
  end
end
