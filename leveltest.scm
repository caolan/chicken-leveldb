(use leveldb)

(define db (leveldb-open "./testdb"))
(define key (list->string '(#\f #\o #\o #\nul)))
(define val (list->string '(#\b #\a #\r #\nul)))

(leveldb-put db key val)
(write (leveldb-get db key))
