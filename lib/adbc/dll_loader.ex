defmodule Adbc.Nif.DLLLoader do
  @moduledoc false
  @on_load :__on_load__
  def __on_load__ do
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
      {:win32, _} ->
        :erlang.nif_error(:not_loaded)

      _ ->
        :ok
    end
  end
end
