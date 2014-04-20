# leveldb

Bindings to [LevelDB][1], a fast and lightweight key/value database library by
Google.

## API

### Open and close

```scheme
(open-db loc #!key (create #t) (exists #t))
```

Opens database with path `loc` and returns a database object. By default,
this method will create the database if it does not exist at `loc` and will
not error if the database already exists. This behaviour can be modified
using the keyword arguments. Setting `exists` to `#f` will mean an
exception occurs if the database already exists. Setting `create` to `#f`
will mean an exception occurs if the database does not exist.

```scheme
(close-db db)
```

Closes database `db`.

```scheme
(call-with-db loc proc #!key (create #t) (error #t))
```

Opens database at `loc` and calls (proc db). The database will be closed when
proc returns or raises an exception.

### Read and Write

```scheme
(db-get db key)
```

Returns the value of `key` in `db` as a string. Causes an exception if the
key does not exist.

```scheme
(db-put db key value #!key (sync #f))
```

Stores `value` under `key` in datbase `db`. If the sync option can be set to
`#t` to make the write operation not return until the data being written has
been pushed all the way to persistent storage. See the *Synchronous Writes*
section for more information.

```scheme
(db-delete db key #!key (sync #f))
```

Removes the value associated with `key` from `db`. If the sync option can be
set to `#t` to make the write operation not return until the data being
written has been pushed all the way to persistent storage. See the
*Synchronous Writes* section for more information.

### Atomic updates

```scheme
(db-batch db ops #!key (sync #f))
```

When making multiple changes that rely on each other you can apply a batch
of operations atomically using `db-batch`. The `ops` argument is a list of
operations which will be applied **in order** (meaning you can create then
later delete a value in the same batch, for example).

```scheme
(define myops '((put "abc" "123")
                (put "def" "456")
                (delete "abc")))

;; apply all operations in myops
(db-batch db myops)
```

The first item in an operation should be the symbol `put` or `delete`, any
other value will give an error. The next item is the key and in the case of
`put` the third item is the value.

Apart from its atomicity benefits, `db-batch` may also be used to speed up
bulk updates by placing lots of individual mutations into the same batch.

### Synchronous Writes

**Note:** this information is mostly copied from the [LevelDB docs][2]

By default, each write to leveldb is asynchronous: it returns after pushing
the write from the process into the operating system. The transfer from
operating system memory to the underlying persistent storage happens
asynchronously. The sync flag can be turned on for a particular write to
make the write operation not return until the data being written has been
pushed all the way to persistent storage. (On Posix systems, this is
implemented by calling either fsync(...) or fdatasync(...) or msync(...,
MS\_SYNC) before the write operation returns.)

Asynchronous writes are often more than a thousand times as fast as
synchronous writes. The downside of asynchronous writes is that a
crash of the machine may cause the last few updates to be lost. Note
that a crash of just the writing process (i.e., not a reboot) will
not cause any loss since even when sync is false, an update is pushed
from the process memory into the operating system before it is
considered done.

`db-batch` provides an alternative to asynchronous writes. Multiple
updates may be placed in the same batch and applied together
using a `sync: #t`. The extra cost of the synchronous write will be
amortized across all of the writes in the batch. 


[1]: https://code.google.com/p/leveldb/
[2]: http://leveldb.googlecode.com/svn/trunk/doc/index.html
