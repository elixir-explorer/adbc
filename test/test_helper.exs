Adbc.download_driver!(:sqlite)
Adbc.download_driver!(:duckdb)

exclude =
  if System.find_executable("psql") do
    Adbc.download_driver!(:postgresql)
    []
  else
    [:postgresql]
  end

exclude =
  if System.get_env("CI") == nil do
    [:mocks | exclude]
  else
    mock_version = "1.0.0"
    with {:ok, triplet} <- Adbc.Driver.current_triplet() do
      name = "libadbc_driver_mocks"
      filename =
        case triplet do
          "x86_64-apple-darwin" ->
            "#{name}-#{triplet}.dylib"

          "aarch64-apple-darwin" ->
            "#{name}-#{triplet}.dylib"

          "x86_64-linux-gnu" ->
            "#{name}-#{triplet}.so"

          "x86_64-windows-msvc" ->
            "#{name}-#{triplet}.dll"

          _ ->
            nil
        end
        if filename do
          cache_path = Path.join([__DIR__, filename])
          if File.exists?(cache_path) do
            System.put_env("ADBC_DRIVER_MOCKS", cache_path)
            []
          else
            url =
              "https://github.com/cocoa-xu/adbc_driver_mocks/releases/download/v#{mock_version}/#{filename}"

            with {:ok, body} <- Adbc.Helper.download(url, false) do
              cache_path = Path.join([__DIR__, filename])
              File.write!(cache_path, body)
              System.put_env("ADBC_DRIVER_MOCKS", cache_path)
              []
            else
              _ ->
                [:mocks | exclude]
            end
          end
        else
          [:mocks | exclude]
        end
    else
      _ ->
        [:mocks | exclude]
    end
    exclude
  end

ExUnit.start(exclude: exclude)
