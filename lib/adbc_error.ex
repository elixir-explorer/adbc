defmodule Adbc.Error do
  defexception [:message, :vendor_code, :state]

  @type t :: %Adbc.Error{
          message: String.t(),
          vendor_code: integer(),
          state: <<_::40>>
        }
end
