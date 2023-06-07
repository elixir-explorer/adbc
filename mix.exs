defmodule ADBC.MixProject do
  use Mix.Project

  @app :adbc
  @version "0.1.0"
  @github_url "https://github.com/cocoa-xu/adbc"

  def project do
    [
      app: @app,
      version: @version,
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      docs: docs(),
      compilers: [:elixir_make] ++ Mix.compilers(),
      make_precompiler: {:nif, CCPrecompiler},
      make_precompiler_url: "#{@github_url}/releases/download/v#{@version}/@{artefact_filename}",
      make_precompiler_filename: "adbc_nif"
    ]
  end

  def application do
    [
      extra_applications: [:logger, inets: :optional, ssl: :optional],
    ]
  end

  defp deps do
    [
      # compilation
      {:cc_precompiler, "~> 0.1.0", runtime: false},
      {:elixir_make, "~> 0.7.0", runtime: false},

      # runtime
      {:dll_loader_helper, "~> 0.1"},
      {:castore, "~> 0.1 or ~> 1.0"},

      # docs
      {:ex_doc, "~> 0.29", only: :docs, runtime: false}
    ]
  end

  defp docs do
    [
      main: "Adbc.Database",
      source_ref: "v#{@version}",
      source_url: @github_url
    ]
  end

  defp package() do
    [
      name: "adbc",
      files:
        ~w(3rd_party/apache-arrow-adbc c_src lib mix.exs README* LICENSE* Makefile checksum.exs),
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => @github_url}
    ]
  end
end
