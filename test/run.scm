(use leveldb posix test lazy-seq)

; attempting to open db that doesn't exist
(if (directory? "testdb")
  (delete-directory "testdb" #t))

(test-group "basic operation"
  (test-error "opening missing db should error when create_if_missing: #f"
              (open-db "testdb" create_if_missing: #f))

  (define db (open-db "testdb"))

  (test "put then get value" "bar"
        (begin
          (db-put db "foo" "bar")
          (db-get db "foo")))

  (let ([key (list->string '(#\f #\o #\o #\nul))]
        [val (list->string '(#\b #\a #\r #\nul))])
      (db-put db key val)
      (db-put db key val)
      (test "null bytes in keys and values" val (db-get db key)))

  (test-error "attempt to get missing key" (db-get db "asdf"))

  ;; delete previously added keys
  (db-delete db "foo")
  (db-delete db (list->string '(#\f #\o #\o #\nul)))
  (test-error "attempt to get foo after deleting should error" (db-get db "foo"))

  (define ops '((put "1" "one")
                (put "2" "two")
                (put "3" "three")))

  (db-batch db ops)
  (test "get 1 after batch" "one" (db-get db "1"))
  (test "get 2 after batch" "two" (db-get db "2"))
  (test "get 3 after batch" "three" (db-get db "3"))

  (test "stream from 2 limit 1"
        '(("2" "two"))
        (db-stream db lazy-seq->list start: "2" limit: 1))

  (test "stream from 2 limit 2"
        '(("2" "two") ("3" "three"))
        (db-stream db lazy-seq->list start: "2" limit: 2))

  (test "stream from start limit 2"
        '(("1" "one") ("2" "two"))
        (db-stream db lazy-seq->list limit: 2))

  (test "stream from start no limit"
        '(("1" "one") ("2" "two") ("3" "three"))
        (db-stream db lazy-seq->list))

  (test "stream from start no limit end 2"
        '(("1" "one") ("2" "two"))
        (db-stream db lazy-seq->list end: "2"))

  (test-error "opening existing db should error when error_if_exists: #t"
              (open-db "testdb" error_if_exists: #t))

  (test-assert "opening existing db should not error by default"
               (close-db (open-db "testdb")))
  (close-db db))

(test-exit)
