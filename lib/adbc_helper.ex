defmodule Adbc.Helper do
  @moduledoc false

  def get_keyword!(opts, key, type, options \\ []) when is_atom(key) do
    val = opts[key] || options[:default] || nil
    allow_nil? = options[:allow_nil] || false
    must_in = options[:must_in] || nil

    if allow_nil? and val == nil do
      val
    else
      case {get_keyword(key, val, type), is_list(must_in)} do
        {{:ok, val}, true} ->
          if Enum.member?(must_in, val) do
            val
          else
            raise ArgumentError, """
            expect keyword parameter `#{inspect(key)}` to be one of the following values,
            #{inspect(must_in)}
            """
          end

        {{:ok, val}, false} ->
          val

        {{:error, reason}, _} ->
          raise ArgumentError, reason
      end
    end
  end

  defp get_keyword(key, val, [:string]) when is_list(val) do
    if Enum.all?(val, fn v -> is_binary(v) end) do
      {:ok, val}
    else
      {:error,
       "expect keyword parameter `#{inspect(key)}` to be a list of string, got `#{inspect(val)}`"}
    end
  end

  defp get_keyword(_key, val, :non_neg_integer) when is_integer(val) and val >= 0 do
    {:ok, val}
  end

  defp get_keyword(key, val, :non_neg_integer) do
    {:error,
     "expect keyword parameter `#{inspect(key)}` to be a non-negative integer, got `#{inspect(val)}`"}
  end

  defp get_keyword(_key, val, :pos_integer) when is_integer(val) and val > 0 do
    {:ok, val}
  end

  defp get_keyword(key, val, :pos_integer) do
    {:error,
     "expect keyword parameter `#{inspect(key)}` to be a positive integer, got `#{inspect(val)}`"}
  end

  defp get_keyword(_key, val, :integer) when is_integer(val) do
    {:ok, val}
  end

  defp get_keyword(key, val, :integer) do
    {:error, "expect keyword parameter `#{inspect(key)}` to be an integer, got `#{inspect(val)}`"}
  end

  defp get_keyword(_key, val, :boolean) when is_boolean(val) do
    {:ok, val}
  end

  defp get_keyword(key, val, :boolean) do
    {:error, "expect keyword parameter `#{inspect(key)}` to be a boolean, got `#{inspect(val)}`"}
  end

  defp get_keyword(_key, val, :function) when is_function(val) do
    {:ok, val}
  end

  defp get_keyword(key, val, :function) do
    {:error, "expect keyword parameter `#{inspect(key)}` to be a function, got `#{inspect(val)}`"}
  end

  defp get_keyword(_key, val, {:function, arity})
       when is_integer(arity) and arity >= 0 and is_function(val, arity) do
    {:ok, val}
  end

  defp get_keyword(key, val, {:function, arity}) when is_integer(arity) and arity >= 0 do
    {:error,
     "expect keyword parameter `#{inspect(key)}` to be a function that can be applied with #{arity} number of arguments , got `#{inspect(val)}`"}
  end

  defp get_keyword(_key, val, :atom) when is_atom(val) do
    {:ok, val}
  end

  defp get_keyword(key, val, {:atom, allowed_atoms})
       when is_atom(val) and is_list(allowed_atoms) do
    if val in allowed_atoms do
      {:ok, val}
    else
      {:error,
       "expect keyword parameter `#{inspect(key)}` to be an atom and is one of `#{inspect(allowed_atoms)}`, got `#{inspect(val)}`"}
    end
  end

  def shared_driver_path(driver, opts \\ []) do
    Adbc.Driver.driver_filepath(driver, opts)
  end

  def get_current_triplet do
    current_target(:os.type())
  end

  defp current_target({:win32, _}) do
    processor_architecture =
      String.downcase(String.trim(System.get_env("PROCESSOR_ARCHITECTURE")))

    # https://docs.microsoft.com/en-gb/windows/win32/winprog64/wow64-implementation-details?redirectedfrom=MSDN
    partial_triplet =
      case processor_architecture do
        "amd64" ->
          "x86_64-windows-"

        "ia64" ->
          "ia64-windows-"

        "arm64" ->
          "aarch64-windows-"

        "x86" ->
          "x86-windows-"
      end

    {compiler, _} = :erlang.system_info(:c_compiler_used)

    case compiler do
      :msc ->
        {:ok, partial_triplet <> "msvc"}

      :gnuc ->
        {:ok, partial_triplet <> "gnu"}

      other ->
        {:ok, partial_triplet <> Atom.to_string(other)}
    end
  end

  defp current_target({:unix, _}) do
    # get current target triplet from `:erlang.system_info/1`
    system_architecture = to_string(:erlang.system_info(:system_architecture))
    current = String.split(system_architecture, "-", trim: true)

    case length(current) do
      4 ->
        {:ok, "#{Enum.at(current, 0)}-#{Enum.at(current, 2)}-#{Enum.at(current, 3)}"}

      3 ->
        case :os.type() do
          {:unix, :darwin} ->
            # could be something like aarch64-apple-darwin21.0.0
            # but we don't really need the last 21.0.0 part
            if String.match?(Enum.at(current, 2), ~r/^darwin.*/) do
              {:ok, "#{Enum.at(current, 0)}-#{Enum.at(current, 1)}-darwin"}
            else
              {:ok, system_architecture}
            end

          _ ->
            {:ok, system_architecture}
        end

      _ ->
        {:error, "cannot decide current target"}
    end
  end

  def download(url, ignore_proxy, filename) do
    cache_path = Path.join(adbc_cache_dir(), filename)

    if File.exists?(cache_path) do
      {:ok, cache_path}
    else
      with {:ok, body} <- uncached_download(url, ignore_proxy),
           :ok <- File.write(cache_path, body) do
        {:ok, cache_path}
      end
    end
  end
  
  defp uncached_download(url, ignore_proxy) do
    url_charlist = String.to_charlist(url)

    {:ok, _} = Application.ensure_all_started(:inets)
    {:ok, _} = Application.ensure_all_started(:ssl)
    {:ok, _} = Application.ensure_all_started(:public_key)

    if !ignore_proxy do
      if proxy = System.get_env("HTTP_PROXY") || System.get_env("http_proxy") do
        %{host: host, port: port} = URI.parse(proxy)
        :httpc.set_options([{:proxy, {{String.to_charlist(host), port}, []}}])
      end

      if proxy = System.get_env("HTTPS_PROXY") || System.get_env("https_proxy") do
        %{host: host, port: port} = URI.parse(proxy)
        :httpc.set_options([{:https_proxy, {{String.to_charlist(host), port}, []}}])
      end
    end

    # https://erlef.github.io/security-wg/secure_coding_and_deployment_hardening/inets
    # TODO: This may no longer be necessary from Erlang/OTP 25.0 or later.
    https_options = [
      ssl:
        [
          verify: :verify_peer,
          customize_hostname_check: [
            match_fun: :public_key.pkix_verify_hostname_match_fun(:https)
          ]
        ] ++ cacerts_options()
    ]

    options = [body_format: :binary]

    case :httpc.request(:get, {url_charlist, []}, https_options, options) do
      {:ok, {{_, 200, _}, _, body}} -> {:ok, body}
      other -> {:error, "couldn't fetch and cache file from #{url}: #{inspect(other)}"}
    end
  end

  defp cacerts_options do
    cond do
      path = System.get_env("ADBC_CACERT") ->
        [cacertfile: path]

      Application.spec(:castore, :vsn) ->
        [cacertfile: Application.app_dir(:castore, "priv/cacerts.pem")]

      Application.spec(:certifi, :vsn) ->
        [cacertfile: Application.app_dir(:certifi, "priv/cacerts.pem")]

      certs = otp_cacerts() ->
        [cacerts: certs]

      path = cacerts_from_os() ->
        [cacertfile: path]

      true ->
        []
    end
  end

  defp otp_cacerts do
    if System.otp_release() >= "25" do
      # cacerts_get/0 raises if no certs found
      try do
        :public_key.cacerts_get()
      rescue
        _ ->
          nil
      end
    end
  end

  # https_opts and related code are taken from
  # https://github.com/elixir-cldr/cldr_utils/blob/v2.19.1/lib/cldr/http/http.ex
  @certificate_locations [
    # Debian/Ubuntu/Gentoo etc.
    "/etc/ssl/certs/ca-certificates.crt",

    # Fedora/RHEL 6
    "/etc/pki/tls/certs/ca-bundle.crt",

    # OpenSUSE
    "/etc/ssl/ca-bundle.pem",

    # OpenELEC
    "/etc/pki/tls/cacert.pem",

    # CentOS/RHEL 7
    "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",

    # Open SSL on MacOS
    "/usr/local/etc/openssl/cert.pem",

    # MacOS & Alpine Linux
    "/etc/ssl/cert.pem"
  ]

  defp cacerts_from_os do
    Enum.find(@certificate_locations, &File.exists?/1)
  end

  def driver_directory do
    case :os.type() do
      {:win32, _} ->
        "#{:code.priv_dir(:adbc)}/bin"

      _ ->
        "#{:code.priv_dir(:adbc)}/lib"
    end
  end

  defp adbc_cache_dir() do
    if dir = System.get_env("ADBC_CACHE_DIR") do
      Path.expand(dir)
    else
      :filename.basedir(:user_cache, "adbc")
    end
  end
end
