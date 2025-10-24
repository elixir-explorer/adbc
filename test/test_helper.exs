Adbc.download_driver!(:sqlite)
Adbc.download_driver!(:duckdb)

pg_exclude =
  if System.find_executable("psql") do
    Adbc.download_driver!(:postgresql)
    []
  else
    [:postgresql]
  end

windows_exclude =
  case :os.type() do
    {:win32, _} -> [:unix]
    _ -> []
  end

ExUnit.start(exclude: pg_exclude ++ windows_exclude)
