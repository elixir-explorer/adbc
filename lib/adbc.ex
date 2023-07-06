defmodule Adbc do
  @moduledoc """
  Documentation for `Adbc`.
  """

  @spec download_driver(atom, keyword) :: :ok | {:error, binary}
  def download_driver(driver, opts \\ []) when is_atom(driver) and is_list(opts) do
    Adbc.Driver.download(driver, opts)
  end

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
