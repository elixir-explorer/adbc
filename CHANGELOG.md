# CHANGELOG

## v0.6.0

#### Breaking changes
* To avoid allocating data twice for inputs, the results now by default will return references in the data field of `Adbc.Column`. 
  
  If you need to use them in the Elixir world, you can use `Adbc.Result.materialize/1` and 
  `Adbc.Column.materialize/1` to convert the data to regular Elixir terms.

  ```elixir
  iex> {:ok, results} = Connection.query(conn, "SELECT 123 as num, 456.78 as fp")
  {:ok,
    results = %Adbc.Result{
      data: [
          %Adbc.Column{
            name: "num",
            type: :s64,
            metadata: nil,
            nullable: true,
            data: [#Reference<0.351247108.3006922760.20174>]
          },
          %Adbc.Column{
            name: "fp",
            type: :f64,
            metadata: nil,
            nullable: true,
            data: [#Reference<0.351247108.3006922760.20175>]
          }
      ],
      num_rows: nil
  }}
  iex> Adbc.Result.materialize(results)
  %Adbc.Result{
    data: [
      %Adbc.Column{
        name: "num",
        type: :s64,
        nullable: true,
        metadata: nil,
        data: [123]
      },
      %Adbc.Column{
        name: "fp",
        type: :f64,
        nullable: true,
        metadata: nil,
        data: [456.78]
      }
    ]
  }
  ```

## v0.5.0

#### Breaking changes
* Signed integer types and functions are now renamed from `s{8,16,32,64}` to `i{8,16,32,64}`

#### Changes
* Updated to ADBC library 12.

#### Fix
* Boolean arrays are now correctly parsed.
* Include `CMakeLists.txt` in package files

#### Added
* Added support for dictionary encoded array

## v0.4.2

#### Added
* Added support for run-end encoded array

#### Fix
* Include `CMakeLists.txt` in package files

## v0.4.1

#### Fix
* Remove debug print

## v0.4.0

#### Changes
* Use `Adbc.Column` for inputs and outputs of result sets

  `Adbc.Column` is a new module that represents columns in a result set. It
  supports all primitive types and lists of primitive types (nested list support
  is not yet implemented).

  Use `Adbc.Result.to_map/1`` to convert a result set to a maps of lists, where
  each key is a column name and the value is a list of values for that column.

* Add `ADBC_CACERTS_PATH` to allow for custom certs (#75)
* Support bind named parameters via `Adbc.Column` (#84)
* Expose more APIs (#64)
  - `Adbc.Database.get_option/2`
  - `Adbc.{Database,Connection}.{get,set}_option`
  - `Adbc.Connection.query_with_options/4`

#### Fixes
* Fix: set parameter length (rows) correctly (#85)
* Fix: parsing dense/sparse union (#63)

## v0.3.1

* Fix erroneous ADBC driver URLs

## v0.3.0

* Update to ADBC 0.11.0-rc0
* Builtin support for DuckDB
* Fix tables in inline docs of `Adbc.Connection` (@lkarthee in #57)
* Default to erlang certificate store (@lkarthee in #57)

## v0.2.3

* Update to ADBC 0.10.0
* Using NIF version 2.16 for all precompilation targets (@cocoa-xu in #54)
* Add API for prepared queries (@dlindenkreuz in #51)

## v0.2.2

* Update to ADBC 0.8.0

## v0.2.1

* Fix URLs for downloading drivers

## v0.2.0

* Update to ADBC 0.7.0
* Parse date, time, and timestamps

## v0.1.1

* Add `Adbc.Connection.get_driver/1`
* Add `Adbc.Connection.query!/3`

## v0.1.0

* Initial release.
