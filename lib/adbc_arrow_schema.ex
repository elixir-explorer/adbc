defmodule Adbc.ArrowSchema do
  @moduledoc """
  Documentation for `Adbc.ArrowSchema`.
  """

  @typedoc """
  - **format**: `String.t()`

    A null-terminated, UTF8-encoded string describing the data type.

    If the data type is nested, child types are not encoded here but
    in the ArrowSchema.children structures.

    Consumers MAY decide not to support all data types, but they should
    document this limitation.

  - **name**: `String.t()`

    A null-terminated, UTF8-encoded string of the field or array name.
    This is mainly used to reconstruct child fields of nested types.

    Producers MAY decide not to provide this information, and consumers
    MAY decide to ignore it. If omitted, MAY be NULL or an empty string.

  - **metadata**: `String.t()` | nil

    A binary string describing the type’s metadata. If the data type is
    nested, child types are not encoded here but in the ArrowSchema.children
    structures.

    This string is not null-terminated but follows a specific format:

    ```
    int32: number of key/value pairs (noted N below)
    int32: byte length of key 0
    key 0 (not null-terminated)
    int32: byte length of value 0
    value 0 (not null-terminated)
    ...
    int32: byte length of key N - 1
    key N - 1 (not null-terminated)
    int32: byte length of value N - 1
    value N - 1 (not null-terminated)
    ```

    Integers are stored in native endianness. For example, the metadata
    `[('key1', 'value1')]` is encoded on a little-endian machine as:

    ```
    \x01\x00\x00\x00\x04\x00\x00\x00key1\x06\x00\x00\x00value1
    ```

    On a big-endian machine, the same example would be encoded as:

    ```
    \x00\x00\x00\x01\x00\x00\x00\x04key1\x00\x00\x00\x06value1
    ```

    If omitted, this field MUST be `nil` (not an empty string).

    Consumers MAY choose to ignore this information.

  - **flags**: `integer()`

    A bitfield of flags enriching the type description. Its value
    is computed by OR’ing together the flag values. The following
    flags are available:

    - `2` (ARROW_FLAG_NULLABLE): whether this field is semantically nullable
      (regardless of whether it actually has null values).

    - `1` (ARROW_FLAG_DICTIONARY_ORDERED): for dictionary-encoded types,
      whether the ordering of dictionary indices is semantically meaningful.

    - `4` (ARROW_FLAG_MAP_KEYS_SORTED): for map types, whether the keys within
      each map value are sorted.

    If omitted, MUST be 0.

    Consumers MAY choose to ignore some or all of the flags. Even then, they
    SHOULD keep this value around so as to propagate its information to their
    own consumers.

  - **n_children**: `integer()`

    The number of children this type has.

  - **reference**: `reference`.

    The underlying erlang resource variable.

  """
  @type t :: %__MODULE__{
          format: String.t(),
          name: String.t(),
          metadata: String.t(),
          flags: integer(),
          n_children: integer(),
          children: [Adbc.ArrowSchema.t() | {:error, String.t()}],
          reference: reference()
        }
  defstruct [:format, :name, :metadata, :flags, :n_children, :children, :reference]
  alias __MODULE__, as: T

  def from_metainfo({format, name, metadata, flags, n_children, children}) do
    %T{
      format: format,
      name: name,
      metadata: metadata,
      flags: flags,
      n_children: n_children,
      children: Enum.map(children, &from_metainfo/1)
    }
  end
end
