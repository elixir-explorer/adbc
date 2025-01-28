defmodule Adbc.DLLLoaderNif do
  # We don't use on_load callbacks because this module
  # needs to be invoked from Adbc.Nif and if both have
  # @on_load the order is not guaranteed in a release.
  @moduledoc false

  @on_load :load_nif
  def load_nif do
    case :os.type() do
      {:win32, _} ->
        priv_dir = :code.priv_dir(:adbc)
        File.mkdir_p!(Path.join(priv_dir, "bin"))
        path = :filename.join(priv_dir, ~c"adbc_dll_loader")
        :erlang.load_nif(path, 0)
        add_dll_directory()

      _ ->
        :ok
    end
  end

  def add_dll_directory do
    case :os.type() do
      {:win32, _} -> :erlang.nif_error(:not_loaded)
      _ -> :ok
    end
  end
end
