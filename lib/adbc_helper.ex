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

  def error_to_exception(%Adbc.Error{} = exception) do
    exception
  end

  def option(callee, func, args)

  def option(pid, func, args) when is_pid(pid) do
    case GenServer.call(pid, {:option, func, args}) do
      :ok ->
        :ok

      {:ok, value} ->
        {:ok, value}

      {:error, reason} ->
        {:error, error_to_exception(reason)}
    end
  end

  def option(ref, func, [type, key | args]) when is_reference(ref) do
    with {:error, reason} <- apply(Adbc.Nif, func, [ref, type, to_string(key) | args]) do
      {:error, error_to_exception(reason)}
    end
  end

  def option_ok_or_halt(callee, func, args) do
    case option(callee, func, args) do
      :ok -> {:cont, :ok}
      {:error, _} = error -> {:halt, error}
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
      custom_certs = System.get_env("ADBC_CACERTS_PATH") ->
        [cacertfile: custom_certs]

      certs = otp_cacerts() ->
        [cacerts: certs]

      true ->
        IO.warn("""
        No certificate trust store was found.

        A certificate trust store is required in
        order to download locales for your configuration.
        Since elixir_make could not detect a system
        installed certificate trust store one of the
        following actions may be taken:

        1. Use OTP 25+ on an OS that has built-in certificate trust store.

        2. Set ADBC_CACERTS_PATH environment variable pointing to a certificate.

        """)

        []
    end
  end

  defp otp_cacerts do
    :public_key.cacerts_get()
  rescue
    _ -> nil
  end
end
