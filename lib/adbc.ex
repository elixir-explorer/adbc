defmodule Adbc do
  @moduledoc """
  Documentation for `Adbc`.
  """
end

for driver <- Application.compile_env(:adbc, :drivers, []) do
  Adbc.Driver.download_driver(driver)
end
