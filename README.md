# leveldb

Bindings to [LevelDB][1], a fast and lightweight key/value database library by
Google. Provides an implementation of the [level][2] egg. Include both eggs to
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

[1]: https://code.google.com/p/leveldb/
[2]: https://github.com/caolan/chicken-level
