Mix.install([{:adbc, "~> 0.3.1-dev", path: "."}])

defmodule BigqueryTest do
  def test do
    children = [
      {Adbc.Database,
      driver: "/Users/cocoa/Workspace/Git/arrow-adbc/build/local/lib/libadbc_driver_bigquery.dylib",
      project_name: "projects/bigquery-poc-418913",
      table_name: "projects/bigquery-poc-418913/datasets/google_trends/tables/small_top_terms",
       process_options: [name: MyApp.DB]},
      {Adbc.Connection,
       database: MyApp.DB,
       process_options: [name: MyApp.Conn]}
    ]
    Supervisor.start_link(children, strategy: :one_for_one)
    dbg(Adbc.Connection.query(MyApp.Conn, ""))
  end
end

BigqueryTest.test()




