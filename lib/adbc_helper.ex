defmodule Adbc.Helper do
  @moduledoc false

  def get_keyword!(opts, key, type, options \\ []) when is_atom(key) do
    val = opts[key] || options[:default] || nil
    allow_nil? = options[:allow_nil] || false
    must_in = options[:must_in] || nil

    if allow_nil? and val == nil do
      val
    else
      case get_keyword(key, val, type) do
        {:ok, val} ->
          val

        {:error, reason} ->
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

  def list_of_binary(data) when is_list(data) do
    count = Enum.count(data)

    if count > 0 do
      first = Enum.at(data, 0)

      if is_binary(first) do
        expected_size = byte_size(first)

        if rem(expected_size, HNSWLib.Nif.float_size()) != 0 do
          raise ArgumentError,
                "vector feature size should be a multiple of #{HNSWLib.Nif.float_size()} (sizeof(float))"
        else
          features = trunc(expected_size / HNSWLib.Nif.float_size())

          if list_of_binary(data, expected_size) == false do
            raise ArgumentError, "all vectors in the input list should have the same size"
          else
            {count, features}
          end
        end
      end
    else
      {0, 0}
    end
  end

  defp list_of_binary([elem | rest], expected_size) when is_binary(elem) do
    if byte_size(elem) == expected_size do
      list_of_binary(rest, expected_size)
    else
      false
    end
  end

  defp list_of_binary([], expected_size) do
    expected_size
  end
end
