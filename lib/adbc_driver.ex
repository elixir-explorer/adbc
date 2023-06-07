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
        {status, info} =
          case triplet do
            "aarch64-apple-darwin" ->
              {:ok,
               base_url <>
                 "#{version}/adbc_driver_#{official_driver}-#{version}-py3-none-macosx_11_0_arm64.whl"}

            "x86_64-apple-darwin" ->
              {:ok,
               base_url <>
                 "#{version}/adbc_driver_#{official_driver}-#{version}-py3-none-macosx_10_9_x86_64.whl"}

            "aarch64-linux-gnu" ->
              {:ok,
               base_url <>
                 "#{version}/adbc_driver_#{official_driver}-#{version}-py3-none-manylinux_2_17_aarch64.manylinux2014_aarch64.whl"}

            "x86_64-linux-gnu" ->
              {:ok,
               base_url <>
                 "#{version}/adbc_driver_#{official_driver}-#{version}-py3-none-manylinux_2_17_x86_64.manylinux2014_x86_64.whl"}

            "x86_64-windows-msvc" ->
              {:ok,
               base_url <>
                 "#{version}/adbc_driver_#{official_driver}-#{version}-py3-none-win_amd64.whl"}

            _ ->
              {:error, "official driver does not have a precompiled version for `#{triplet}`"}
          end

        if status == :ok do
          do_download(info, triplet, ignore_proxy, official_driver)
        else
          {:error, info}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp do_download(url, triplet, ignore_proxy, driver_name) do
    with {:ok, zip_data} <- Helper.download(url, ignore_proxy),
         {:ok, zip_handle} <- :zip.zip_open(zip_data, [:cooked, :memory]) do
      for {:ok, zip_files} <- [:zip.table(zip_data, [:cooked])],
          {:zip_file, filename, _, _, _, _} <- zip_files,
          Path.extname(filename) == ".so" do
        with {:ok, {filename, file_data}} <- :zip.zip_get(filename, zip_handle) do
          filename =
            String.replace_leading(
              to_string(filename),
              "adbc_driver_#{driver_name}/",
              "#{triplet}-"
            )
          filepath = Path.join(Helper.driver_directory(), filename),
          File.write(filepath, file_data)
        end
      end

      :zip_close(zip_handle)
    end
  end

  def driver_filepath(driver_name) do
    case Helper.get_current_triplet() do
      {:ok, triplet} ->
        {:ok, Path.join(Helper.driver_directory(), "#{triplet}-libadbc_driver_#{driver_name}.so")}

      {:error, reason} ->
        {:error, reason}
    end
  end
end
