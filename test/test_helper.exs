Adbc.download_driver!(:sqlite)

exclude =
  if System.find_executable("psql") do
    Adbc.download_driver!(:postgresql)
    []
  else
    [:postgresql]
  end

ExUnit.start(exclude: exclude)
