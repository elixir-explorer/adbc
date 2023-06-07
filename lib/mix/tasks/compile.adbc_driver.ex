defmodule Mix.Tasks.Compile.AdbcDriver do
  @moduledoc false

  @doc false
  def run(_args) do
    :ok
  end

  for driver <- Application.compile_env(:adbc, :drivers, [:sqlite, :postgresql]) do
    Adbc.Driver.download_driver(driver)
  end
end
