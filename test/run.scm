(use leveldb posix test)

; attempting to open db that doesn't exist
(if (directory? "testdb")
  (delete-directory "testdb" #t))

(test-error "opening missing db should error when create_if_missing: #f"
            (db-open "testdb" create_if_missing: #f))

(define db (db-open "testdb"))

(test "put then get value" "bar"
      (begin
        (db-put db "foo" "bar")
        (db-get db "foo")))

(let ([key (list->string '(#\f #\o #\o #\nul))]
      [val (list->string '(#\b #\a #\r #\nul))])
    (db-put db key val)
    (test "null bytes in keys and values" val (db-get db key)))

(test-error "attempt to get missing key" (db-get db "asdf"))

(define ops '((put "one" "1")
              (put "two" "2")
              (put "three" "3")))

(db-batch db ops)
(test "get one after batch" "1" (db-get db "one"))
(test "get two after batch" "2" (db-get db "two"))
(test "get three after batch" "3" (db-get db "three"))

(test-error "opening existing db should error when error_if_exists: #t"
            (db-open "testdb" error_if_exists: #t))

(test-assert "opening existing db should not error by default"
             (db-close (db-open "testdb")))

(db-close db)
(test-exit)
