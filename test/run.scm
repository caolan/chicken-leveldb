(use leveldb posix test)

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

  ;(define iter (open-iterator db))
  ;(iter-seek-first! iter)
  ;(test "iter next key is 1" "1" (iter-key iter))
  ;(test "iter next value is one" "one" (iter-value iter))
  ;(test "iter is valid" #t (iter-valid? iter))
  ;(iter-next! iter)
  ;(test "iter next key is 2" "2" (iter-key iter))
  ;(test "iter next value is two" "two" (iter-value iter))
  ;(test "iter is valid" #t (iter-valid? iter))
  ;(iter-next! iter)
  ;(test "iter next key is 3" "3" (iter-key iter))
  ;(test "iter next value is three" "three" (iter-value iter))
  ;(test "iter is valid" #t (iter-valid? iter))
  ;(iter-prev! iter)
  ;(test "iter prev key is 2" "2" (iter-key iter))
  ;(test "iter prev value is two" "two" (iter-value iter))
  ;(test "iter is valid" #t (iter-valid? iter))
  ;(iter-next! iter)
  ;(iter-next! iter)
  ;(test "iter next next is not valid - at end" #f (iter-valid? iter))
  ;(test "status is OK" '(#t "OK") (iter-status iter))
  ;(iter-seek! iter "3")
  ;(test "iter seek 3 key is 3" "3" (iter-key iter))
  ;(test "iter seek 3 value is three" "three" (iter-value iter))
  ;(test "iter is valid" #t (iter-valid? iter))
  ;(close-iterator iter)

  (test-error "opening existing db should error when error_if_exists: #t"
              (open-db "testdb" error_if_exists: #t))

  (test-assert "opening existing db should not error by default"
               (close-db (open-db "testdb")))
  (close-db db))

(test-exit)
