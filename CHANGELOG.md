# CHANGELOG

## v0.4.0-dev

* Add ADBC.Column which can be used as buffer inputs and returned as part of result sets

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
