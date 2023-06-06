defmodule Adbc.Driver do
  alias Adbc.Helper

  def official_driver_base_url,
    do: "https://github.com/apache/arrow-adbc/releases/download/apache-arrow-adbc-"

  def download_driver(official_driver, opts \\ [])
      when official_driver == :sqlite or official_driver == :postgresql or
             official_driver == :flightsql or official_driver == :snowflake do
    base_url = opts[:base_url] || official_driver_base_url()
    version = opts[:version] || "0.4.0"
    ignore_proxy = opts[:ignore_proxy] || false

    case Helper.get_current_triplet() do
      {:ok, triplet} ->
        url =
          case triplet do
            "aarch64-apple-darwin" ->
              base_url <>
                "#{version}/adbc_driver_#{official_driver}-#{version}-py3-none-macosx_11_0_arm64.whl"

            "x86_64-apple-darwin" ->
              base_url <>
                "#{version}/adbc_driver_#{official_driver}-#{version}-py3-none-macosx_10_9_x86_64.whl"

            "aarch64-linux-gnu" ->
              base_url <>
                "#{version}/adbc_driver_#{official_driver}-#{version}-py3-none-manylinux_2_17_aarch64.manylinux2014_aarch64.whl"

            "x86_64-linux-gnu" ->
              base_url <>
                "#{version}/adbc_driver_#{official_driver}-#{version}-py3-none-manylinux_2_17_x86_64.manylinux2014_x86_64.whl"

            "x86_64-windows-msvc" ->
              base_url <>
                "#{version}/adbc_driver_#{official_driver}-#{version}-py3-none-win_amd64.whl"

            _ ->
              {:error, "offifical driver does not have a precompiled version for `#{triplet}`"}
          end

        if is_binary(url) do
          do_download(url, ignore_proxy, official_driver)
        else
          url
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp do_download(url, ignore_proxy, driver_name) do
    with {:ok, zip_data} <- Helper.download(url, ignore_proxy),
         {:ok, zip_handle} <- :zip.zip_open(zip_data, [:cooked, :memory]),
         {:ok, files} <- :zip.table(zip_data, [:cooked]),
         files = Enum.reject(files, fn f -> elem(f, 0) != :zip_file end),
         so_files =
           Enum.reject(files, fn {:zip_file, filename, _, _, _, _} ->
             !String.ends_with?(to_string(filename), ".so")
           end),
         so_files = Enum.map(so_files, fn so_file_entry -> elem(so_file_entry, 1) end) do
      Enum.each(so_files, fn so_file ->
        with {:ok, {filename, file_data}} <- :zip.zip_get(so_file, zip_handle),
             filename =
               String.replace_leading(to_string(filename), "adbc_driver_#{driver_name}/", ""),
             filepath = Path.join(Helper.driver_directory(), filename),
             :ok <- File.write(filepath, file_data) do
          :ok
        end
      end)
    else
      {:error, reason} ->
        {:error, reason}
    end
  end
end
