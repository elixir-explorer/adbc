defmodule Adbc do
  @moduledoc """
  Documentation for `Adbc`.
  """

  @doc """
  Get ADBC function pointer
  """
  @spec get_function_pointer(String.t() | nil) :: map() | non_neg_integer()
  def get_function_pointer(func_name \\ nil) do
    ptr = Adbc.Nif.adbc_get_all_function_pointers()

    if func_name != nil do
      ptr[func_name]
    else
      ptr
    end
  end
end

for driver <- Application.compile_env(:adbc, :drivers, [:sqlite, :postgresql]) do
  Adbc.Driver.download_driver(driver)
end
