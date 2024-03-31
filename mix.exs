defmodule Adbc.MixProject do
  use Mix.Project

  @version "0.3.0-dev"
  @github_url "https://github.com/elixir-explorer/adbc"

  def project do
    [
      app: :adbc,
      version: @version,
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      docs: docs(),
      description: "Apache Arrow ADBC bindings for Elixir",
      compilers: [:elixir_make] ++ Mix.compilers(),
      make_precompiler: {:nif, CCPrecompiler},
      make_precompiler_url: "#{@github_url}/releases/download/v#{@version}/@{artefact_filename}",
      make_precompiler_filename: "adbc_nif",
      make_precompiler_nif_versions: [versions: ["2.16"]],
      cc_precompiler: [
        cleanup: "clean",
        compilers: %{
          {:unix, :linux} => %{
            "x86_64-linux-gnu" => {
              "x86_64-linux-gnu-gcc",
              "x86_64-linux-gnu-g++"
            }
          },
          {:unix, :darwin} => %{
            :include_default_ones => true
          },
          {:win32, :nt} => %{
            :include_default_ones => true
          }
        }
      ]
    ]
  end

  def application do
    [
      extra_applications: [:logger, inets: :optional, ssl: :optional]
    ]
  end

  defp deps do
    [
      # compilation
      {:cc_precompiler, "~> 0.1.8 or ~> 0.2", runtime: false},
      {:elixir_make, "~> 0.8", runtime: false},

      # runtime
      {:dll_loader_helper_beam, "~> 1.0"},
      {:castore, "~> 1.0", optional: true},

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
        ~w(3rd_party/apache-arrow-adbc c_src lib mix.exs README* LICENSE* Makefile Makefile.win checksum.exs),
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => @github_url}
    ]
  end
end
