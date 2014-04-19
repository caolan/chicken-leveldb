# leveldb

Bindings to [LevelDB][1], a fast and lightweight key/value database library by
Google.

## API

### Open and close

```scheme
(open-db loc #!key (create_if_missing #t) (error_if_exists #f))
```

Opens database with path `loc` and returns a database object. By default,
this method will create the database if it does not exist at `loc` and will
not error if the database already exists. This behaviour can be modified
using the keyword arguments.

```scheme
(close-db db)
```

Closes database `db`.

```scheme
(call-with-db loc proc #!key (create_if_missing #t) (error_if_exists #f))
```

Opens database at `loc` and calls (proc db). The database will be closed when
proc returns or raises an exception.

### Read and Write
### Atomic updates
### Synchronous Writes


[1]: https://code.google.com/p/leveldb/
