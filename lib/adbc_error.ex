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
  @type adbc_error :: {:error, {
    message :: String.t(),
    vendor_code :: integer(),
    sqlstate :: << _::40 >>}
  }
end
