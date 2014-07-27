# leveldb

Bindings to [LevelDB][1], a fast and lightweight key/value database library by
Google. Provides an implementation of the
[level](https://github.com/caolan/chicken-level) egg. Include both eggs to
provide the API used in these examples.

## Examples

### Basic operation

```scheme
(use level leveldb)

(define db (open-db "./example"))

(db-put db "hello" "world")
(display (db-get db "hello")) ;; => world
(db-delete db "hello")

(close-db db)
```

### Batches and ranges

```scheme
(use level leveldb lazy-seq)

(define operations
  '((put "name:123" "jane")
    (put "name:456" "joe")))

(define (print-names pairs)
  (lazy-each print pairs))

(call-with-db "./example"
  (lambda (db)
    (db-batch db operations)
    (print-names (db-stream db start: "name:" end: "name::"))))

;; prints
;; => (name:123 jane)
;; => (name:456 joe)
```

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
(call-with-db loc proc #!key (create #t) (exists #t))
```

Opens database at `loc` and calls (proc db). The database will be closed when
proc returns or raises an exception.

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
