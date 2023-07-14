defmodule Adbc.Error do
  @derive {Inspect, only: [:message, :vendor_code]}
  defexception [:message, :vendor_code, :state]

  @type t :: %Adbc.Error{
          message: String.t(),
          vendor_code: integer(),
          state: <<_::40>>
        }
end
