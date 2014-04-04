(use leveldb test)

(define db (leveldb-open "./testdb"))

(test "open db, put then get value"
      "bar"
      (begin
        (leveldb-put db "foo" "bar")
        (leveldb-get db "foo")))

(let ([key (list->string '(#\f #\o #\o #\nul))]
      [val (list->string '(#\b #\a #\r #\nul))])
    (leveldb-put db key val)
    (test "null bytes in keys and values" val (leveldb-get db key)))

(test-exit)
