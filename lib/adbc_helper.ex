defmodule Adbc.Helper do
  @moduledoc false

  @doc false
  def error_to_exception(string) when is_binary(string) do
    ArgumentError.exception(string)
  end

  def error_to_exception(list) when is_list(list) do
    ArgumentError.exception(List.to_string(list))
  end

  def error_to_exception({:adbc_error, message, vendor_code, state}) do
    Adbc.Error.exception(message: message, vendor_code: vendor_code, state: state)
  end

  def get_option(type, :string, ref, key) do
    case type do
      :statement ->
        Adbc.Nif.adbc_statement_get_option(ref, to_string(key))

      :connection ->
        Adbc.Nif.adbc_connection_get_option(ref, to_string(key))

      :database ->
        Adbc.Nif.adbc_database_get_option(ref, to_string(key))
    end
  end

  def get_option(type, :bytes, ref, key) do
    case type do
      :statement ->
        Adbc.Nif.adbc_statement_get_option_bytes(ref, to_string(key))

      :connection ->
        Adbc.Nif.adbc_connection_get_option_bytes(ref, to_string(key))

      :database ->
        Adbc.Nif.adbc_database_get_option_bytes(ref, to_string(key))
    end
  end

  def get_option(type, :int, ref, key) do
    case type do
      :statement ->
        Adbc.Nif.adbc_statement_get_option_int(ref, to_string(key))

      :connection ->
        Adbc.Nif.adbc_connection_get_option_int(ref, to_string(key))

      :database ->
        Adbc.Nif.adbc_database_get_option_int(ref, to_string(key))
    end
  end

  def get_option(type, :double, ref, key) do
    case type do
      :statement ->
        Adbc.Nif.adbc_statement_get_option_double(ref, to_string(key))

      :connection ->
        Adbc.Nif.adbc_connection_get_option_double(ref, to_string(key))

      :database ->
        Adbc.Nif.adbc_database_get_option_double(ref, to_string(key))
    end
  end

  def set_option(type, ref, key, value) when is_binary(value) or is_atom(value) do
    case type do
      :statement ->
        Adbc.Nif.adbc_statement_set_option(ref, to_string(key), to_string(value))

      :connection ->
        Adbc.Nif.adbc_connection_set_option(ref, to_string(key), to_string(value))

      :database ->
        Adbc.Nif.adbc_database_set_option(ref, to_string(key), to_string(value))
    end
  end

  def set_option(type, ref, key, {:bytes, value}) when is_binary(value) do
    case type do
      :statement ->
        Adbc.Nif.adbc_statement_set_option_bytes(ref, to_string(key), value)

      :connection ->
        Adbc.Nif.adbc_connection_set_option_bytes(ref, to_string(key), value)

      :database ->
        Adbc.Nif.adbc_database_set_option_bytes(ref, to_string(key), value)
    end
  end

  def set_option(type, ref, key, value) when is_integer(value) do
    case type do
      :statement ->
        Adbc.Nif.adbc_statement_set_option_int(ref, to_string(key), value)

      :connection ->
        Adbc.Nif.adbc_connection_set_option_int(ref, to_string(key), value)

      :database ->
        Adbc.Nif.adbc_database_set_option_int(ref, to_string(key), value)
    end
  end

  def set_option(type, ref, key, value) when is_float(value) do
    case type do
      :statement ->
        Adbc.Nif.adbc_statement_set_option_double(ref, to_string(key), value)

      :connection ->
        Adbc.Nif.adbc_connection_set_option_double(ref, to_string(key), value)

      :database ->
        Adbc.Nif.adbc_database_set_option_double(ref, to_string(key), value)
    end
  end

  @doc false
  def download(url, ignore_proxy) do
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
      {:ok, {{_, 200, _}, _headers, body}} ->
        {:ok, body}

      other ->
        {:error, "couldn't fetch file from #{url}: #{inspect(other)}"}
    end
  end

  defp cacerts_options do
    cond do
      certs = otp_cacerts() ->
        [cacerts: certs]

      Application.spec(:castore, :vsn) ->
        [cacertfile: Application.app_dir(:castore, "priv/cacerts.pem")]

      true ->
        IO.warn("""
        No certificate trust store was found.

        A certificate trust store is required in
        order to download locales for your configuration.
        Since elixir_make could not detect a system
        installed certificate trust store one of the
        following actions may be taken:

        1. Use OTP 25+ on an OS that has built-in certificate
           trust store.

        2. Install the hex package `castore`. It will
           be automatically detected after recompilation.

        """)

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
end
