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

  def shared_driver_path(driver) do
    {prefix, extension} =
      case :os.type() do
        {:unix, :darwin} ->
          {"lib", "dylib"}

        {:unix, _} ->
          {"lib", "so"}

        {:win32, _} ->
          {"", "dll"}
      end

    otp_app =
      case driver do
        "sqlite" ->
          :adbc

        "postgresql" ->
          :adbc_driver_postgresql

        "flightsql" ->
          :adbc_driver_flightsql

        "snowflake" ->
          :adbc_driver_snowflake
      end

    shared_driver_path(otp_app, prefix, extension)
  end

  defp shared_driver_path(:adbc, prefix, extension) do
    "#{:code.priv_dir(:adbc)}/lib/#{prefix}adbc_driver_sqlite.#{extension}"
  end
end
