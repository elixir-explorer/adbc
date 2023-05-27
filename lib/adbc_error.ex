defmodule Adbc.Error do
  @typedoc """
  adbc_error type

  - `message`: `String.t()`

    The error message.

  - `vendor_code`: `integer()`

    A vendor-specific error code, if applicable.

  - `sqlstate`: `binary()`

    A SQLSTATE error code, if provided, as defined by the
    SQL:2003 standard.  If not set, it should be set to
    `<< "\0\0\0\0\0" >>`.
  """
  @type adbc_error ::
          {:error,
           {
             message :: String.t(),
             vendor_code :: integer(),
             sqlstate :: <<_::40>>
           }}
  @type t :: %__MODULE__{
    reference: reference(),
    pointer: non_neg_integer()
  }
  defstruct [:reference, :pointer]
  alias __MODULE__, as: T

  @doc """
  Create a new zero'ed struct AdbcError
  """
  @spec new :: {:ok, Adbc.Error.t()} | {:error, String.t()}
  def new do
    case Adbc.Nif.adbc_error_new() do
      {:ok, ref, ptr} ->
        {:ok, %T{
          reference: ref,
          pointer: ptr
        }}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Reset the underlying C struct AdbcError to all zero
  so that it can be reused

  If the underlying C struct AdbcError has been set to
  store a previous error, AdbcError::release() will be
  called automatically before memset'ing it to zeros.
  """
  @spec reset(Adbc.Error.t()) :: :ok | {:error, String.t()}
  def reset(self = %T{}) do
    Adbc.Nif.adbc_error_reset(self.reference)
  end

  def to_term(self = %T{}) do
    Adbc.Nif.adbc_error_to_term(self.reference)
  end
end
