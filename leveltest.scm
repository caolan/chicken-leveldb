(use leveldb)

(define db (open-db "./testdb"))
(define key (list->string '(#\f #\o #\o #\nul)))
(define val (list->string '(#\b #\a #\r #\nul)))

(db-put db key val)
(write (db-get db key))
(close-db db)
