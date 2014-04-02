(use leveldb)

(define db (leveldb-open "./testdb"))
(leveldb-put db "foo" "bar")
(write (leveldb-get db "foo"))
