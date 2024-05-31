# CHANGELOG

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
