(use level leveldb posix test lazy-seq test-generative srfi-69 srfi-4)

; attempting to open db that doesn't exist
(if (directory? "testdb")
  (delete-directory "testdb" #t))

(test-group "basic operation"

  (test "opening missing db should error when create_if_missing: #f"
        'does-not-exist
        (condition-case (open-db "testdb" create: #f)
          ((exn leveldb) 'does-not-exist)))

  (define db (open-db "testdb"))
  (test-assert "open-db should return a level record"
               (level? db))

  (test "put then get value" "bar"
        (begin
          (db-put db "foo" "bar")
          (db-get db "foo")))

  (let ((key (list->string '(#\f #\o #\o #\nul)))
        (val (list->string '(#\b #\a #\r #\nul))))
      (db-put db key val)
      (db-put db key val)
      (test "null bytes in keys and values" val (db-get db key)))

  (test-error "error on missing key" (db-get db "asdf"))
  (test "db-get/default on missing key"
        "ASDF"
        (db-get/default db "asdf" "ASDF"))

  ;; delete previously added keys
  (db-delete db "foo")
  (db-delete db (list->string '(#\f #\o #\o #\nul)))
  (test-error "attempt to get foo after deleting should return #f"
              (db-get db "foo"))

  (define ops '((put "1" "one")
                (put "asdf" "asdf")
                (put "2" "two")
                (put "3" "three")
                (delete "asdf")))

  (db-batch db ops)
  (test "get 1 after batch" "one" (db-get db "1"))
  (test "get 2 after batch" "two" (db-get db "2"))
  (test "get 3 after batch" "three" (db-get db "3"))

  (test "stream from 2 limit 1"
        '(("2" . "two"))
        (lazy-seq->list (db-stream db start: "2" limit: 1)))

  (test "stream from 2 limit 2"
        '(("2" . "two") ("3" . "three"))
        (lazy-seq->list (db-stream db start: "2" limit: 2)))

  (test "stream from start limit 2"
        '(("1" . "one") ("2" . "two"))
        (lazy-seq->list (db-stream db limit: 2)))

  (test "stream from start no limit"
        '(("1" . "one") ("2" . "two") ("3" . "three"))
        (lazy-seq->list (db-stream db)))

  (test "stream from start no limit end 2"
        '(("1" . "one") ("2" . "two"))
        (lazy-seq->list (db-stream db end: "2")))

  (test "stream from start 2 limit 2 end 2"
        '(("2" . "two"))
        (lazy-seq->list (db-stream db start: "2" end: "2" limit: 2)))

  (test "stream from start 2 limit 1 end 3"
        '(("2" . "two"))
        (lazy-seq->list (db-stream db start: "2" end: "3" limit: 1)))

  (test "stream keys from start 1 end 3"
        '("1" "2" "3")
        (lazy-seq->list
          (db-stream db start: "1" end: "3" key: #t value: #f)))

  (test "stream values from start 1 end 3"
        '("one" "two" "three")
        (lazy-seq->list
          (db-stream db start: "1" end: "3" key: #f value: #t)))

  (test "stream reverse start 3 end 2"
        '(("3" . "three") ("2" . "two"))
        (lazy-seq->list
          (db-stream db reverse: #t start: "3" end: "2")))

  (test "stream reverse start 3 limit 3"
        '(("3" . "three") ("2" . "two") ("1" . "one"))
        (lazy-seq->list
          (db-stream db reverse: #t start: "3" limit: 3)))

  (db-batch db '((put "four\x00zzz" "000")
                 (put "four\x00def" "456")
                 (put "four\x00abc" "123")
                 (put "three\x00one" "foo")
                 (put "three\x00two" "bar")))
  (test "stream reverse with start, end and keys including nul"
        '(("four\x00zzz" . "000")
          ("four\x00def" . "456")
          ("four\x00abc" . "123"))
        (lazy-seq->list
          (db-stream db reverse: #t start: "four\x00\xff" end: "four\x00")))

  (close-db db)

  (test-error "opening existing db should error when exists: #f"
              (open-db "testdb" exists: #f))

  (test-assert "opening existing db should not error by default"
               (close-db (open-db "testdb")))

  ;; run with valgrind to check call-with-db cleans up
  (test "call-with-db returns value of proc" "one"
        (call-with-db "testdb" (lambda (db) (db-get db "1"))))

  (test-error "call-with-db exceptions exposed"
              (call-with-db "testdb" (lambda (db) (abort "fail")))))

(test-group "throw some random data at it"
  (if (directory? "testdb")
    (delete-directory "testdb" #t))

  (define db (open-db "testdb"))
  (define data (make-hash-table))

  (define (random-string)
    (blob->string
      (u8vector->blob
        (list->u8vector
          (list-tabulate 100 (lambda (i) (random 256)))))))

  (parameterize
    ((current-test-generative-iterations 1000))
    (test-generative ((k random-string) (v random-string))
      (db-batch db (list (list 'put k v)))
      (let ((r (db-get db k)))
        (test "returned matches PUT" v r))))

  (close-db db))

(test-group "large keys/values (100MB)"
  (define db (open-db "testdb"))
  (let ((key (make-string (* 1024 1024 100) #\k))
        (val (make-string (* 1024 1024 100) #\v)))
    (db-put db key val)
    (test "get value" val (db-get db key))
    (test "stream key and value"
          (list (cons key val))
          (lazy-seq->list (db-stream db
                                     start: key
                                     limit: 1))))
  (close-db db))

(test-group "error conditions"
  (define db (open-db "testdb"))
  (test-assert "not-found"
               (condition-case (db-get db "MISSING")
                 ((exn leveldb not-found) #t)
                 (() #f)))
  (test-assert "open locked db"
               (condition-case (open-db "testdb")
                 ((exn leveldb) #t)
                 (() #f))))

(test-exit)
