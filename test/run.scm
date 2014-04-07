(use leveldb posix test)

;; attempting to open db that doesn't exist
(if (directory? "testdb")
  (delete-directory "testdb" #t))

(test-error "opening missing db should error when create_if_missing: #f"
            (leveldb-open "testdb" create_if_missing: #f))

(define db (leveldb-open "testdb"))

(test "open db, put then get value"
      "bar"
      (begin
        (leveldb-put db "foo" "bar")
        (leveldb-get db "foo")))

(let ([key (list->string '(#\f #\o #\o #\nul))]
      [val (list->string '(#\b #\a #\r #\nul))])
    (leveldb-put db key val)
    (test "null bytes in keys and values" val (leveldb-get db key)))

(test-error "attempt to get missing key" (leveldb-get db "asdf"))

(test-error "opening existing db should error when error_if_exists: #t"
            (leveldb-open "testdb" error_if_exists: #t))

(test-assert "opening existing db should not error by default"
             (leveldb-open "testdb"))

(test-exit)
