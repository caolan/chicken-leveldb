(require-extension utf8)

(module leveldb
  (
   leveldb
   call-with-db
   open-db
   close-db
   )

(import utf8 scheme chicken foreign)
(use srfi-1 level interfaces records coops srfi-13 lazy-seq lolevel)

;; Basic implementation of LevelDB interface, using libleveldb
(define leveldb
  (implementation level-api

    (define (get db key)
      (let* ([keystr (string->slice key)]
             [ret (make-stdstr)]
             [status (make-status)]
             [void (c-leveldb-get db keystr ret status)]
             [result (stdstr->string ret)])
        (delete-slice keystr)
        (delete-stdstr ret)
        (check-status status)
        result))

    (define (put db key value #!key (sync #f))
      (let ([keystr (string->slice key)]
            [valstr (string->slice value)]
            [status (make-status)])
        (c-leveldb-put db keystr valstr status sync)
        (delete-slice keystr)
        (delete-slice valstr)
        (check-status status)))

    (define (delete db key #!key (sync #f))
      (let* ([keystr (string->slice key)]
             [status (make-status)]
             [void (c-leveldb-del db keystr status sync)])
        (delete-slice keystr)
        (check-status status)))

    (define (batch db ops #!key (sync #f))
      (let ([batch (make-batch)]
            [status (make-status)])
        (fill-batch batch ops)
        (c-leveldb-write-batch db batch status sync)
        (delete-batch batch)
        (check-status status)))

    (define (stream db
                    #!key
                    start
                    end
                    limit
                    reverse
                    (key #t)
                    (value #t)
                    fillcache)
      (let* ([it (open-iterator db fillcache)])
        (init-stream it start reverse)
        (make-stream it end limit
                     (make-stream-value key value)
                     (stream-start? start reverse)
                     (stream-end? reverse)
                     (if reverse iter-prev! iter-next!))))))


(define (close-db db)
  (if (level? db)
    (close-db (level-resource db))
    (begin
      (map close-iterator (slot-value db 'iterators))
      (set! (slot-value db 'closed) #t)
      ((foreign-lambda* void ((DB db)) "delete db;")
       (slot-value db 'this)))))

(define (open-db loc #!key (create #t) (exists #t))
  (let* ([status (make-status)]
         [db (c-leveldb-open loc status create exists)])
    (check-status status)
    (make-level leveldb db)))

(define (call-with-db loc proc #!key (create #t) (exists #t))
  (let ([db (open-db loc create: create exists: exists)])
    (dynamic-wind (lambda () #f)
                  (lambda () (proc db))
                  (lambda () (close-db db)))))


(foreign-declare "#include <iostream>")
(foreign-declare "#include \"leveldb/db.h\"")
(foreign-declare "#include \"leveldb/write_batch.h\"")

(define-class <db> () ((this '()) (closed #f) (iterators '())))
(define-foreign-type DB (instance "leveldb::DB" <db>))

(define-class <iter> () ((this '()) (db #f)))
(define-foreign-type iter (instance "leveldb::Iterator" <iter>))

(define-class <options> () ((this '())))
(define-foreign-type options (instance "leveldb::Options" <options>))

(define-class <stdstr> () ((this '())))
(define-foreign-type stdstr (instance "std::string" <stdstr>))

(define stdstr-data
  (foreign-lambda* (c-pointer unsigned-char) ((stdstr str))
    "C_return(str->data());"))

(define stdstr-size
  (foreign-lambda* integer ((stdstr  str))
    "C_return(str->size());"))

(define delete-stdstr
  (foreign-lambda* void ((stdstr ret)) "delete ret;"))

(define (string->stdstr str)
  ((foreign-lambda*
     stdstr
     ((integer size) (scheme-pointer data))
     "std::string *x = new std::string((const char*)data, size);
      C_return(x);")
   (byte-string-length str)
   str))

(define (stdstr->string str)
  (let* ([size (stdstr-size str)]
         [data (stdstr-data str)]
         [result (make-string size)])
    (move-memory! data result size)
    result))

(define make-stdstr
  (foreign-lambda* stdstr ()
    "std::string *x = new std::string();
     C_return(x);"))

(define-class <slice> () ((this '())))
(define-foreign-type slice (instance "leveldb::Slice" <slice>))

(define slice-data
  (foreign-lambda* (c-pointer unsigned-char) ((slice s))
    "C_return(s->data());"))

(define slice-size
  (foreign-lambda* integer ((slice s))
    "C_return(s->size());"))

(define delete-slice
  (foreign-lambda* void ((slice s)) "delete s;"))

(define (string->slice str)
  ((foreign-lambda* slice ((integer size) (scheme-pointer data))
     "leveldb::Slice *x = new leveldb::Slice((const char*)data, size);
      C_return(x);")
   (byte-string-length str)
   str))

(define (slice->string s)
  (let* ([size (slice-size s)]
         [data (slice-data s)]
         [result (make-string size)])
    (move-memory! data result size)
    result))

(define make-slice
  (foreign-lambda* slice ()
    "leveldb::Slice *x = new leveldb::Slice();
     C_return(x);"))

(define-class <status> () ((this '())))
(define-foreign-type status (instance "leveldb::Status" <status>))

(define make-status
  (foreign-lambda* status ()
    "leveldb::Status *s = new leveldb::Status();
     C_return(s);"))

(define status-ok?
  (foreign-lambda* bool ((status s)) "C_return(s->ok());"))

(define status-message
  (foreign-lambda* c-string ((status s)) "C_return(s->ToString().c_str());"))

(define delete-status
  (foreign-lambda* void ((status s)) "delete s;"))

(define (make-leveldb-condition subtype msg)
  (make-composite-condition
    (make-property-condition 'exn 'message msg)
    (make-property-condition 'leveldb)
    (make-property-condition subtype)))

(define (status-subtype msg)
  (cond
    [(string-prefix? "NotFound: " msg) 'not-found]
    [(string-prefix? "Corruption: " msg) 'corruption]
    [(string-prefix? "Not implemented: " msg) 'not-implemented]
    [(string-prefix? "Invalid argument: " msg) 'invalid-argument]
    [(string-prefix? "IO error: " msg) 'io-error]
    [else 'error]))

(define (status->condition msg)
  (make-leveldb-condition (status-subtype msg) msg))

(define (check-status s)
  (if (status-ok? s)
    (begin (delete-status s) #t)
    (let ([msg (status-message s)])
      (delete-status s)
      (abort (status->condition msg)))))

(define c-leveldb-open
  (foreign-lambda* DB ((c-string loc) (status s) (bool create) (bool exists))
    "leveldb::DB* db;
     leveldb::Options options;
     options.create_if_missing = create;
     options.error_if_exists = !exists;
     *s = leveldb::DB::Open(options, loc, &db);
     C_return(db);"))

(define c-leveldb-put
  (foreign-lambda* void ((DB db) (slice key) (slice value) (status s) (bool sync))
    "leveldb::WriteOptions write_options;
     write_options.sync = sync;
     *s = db->Put(write_options, *key, *value);"))

(define c-leveldb-get
  (foreign-lambda* void ((DB db) (slice key) (stdstr ret) (status s))
    "*s = db->Get(leveldb::ReadOptions(), *key, ret);"))

(define c-leveldb-del
  (foreign-lambda* void ((DB db) (slice key) (status s) (bool sync))
    "leveldb::WriteOptions write_options;
     write_options.sync = sync;
     *s = db->Delete(write_options, *key);"))

(define-class <batch> () ((this '())))
(define-foreign-type batch (instance "leveldb::WriteBatch" <batch>))

(define make-batch
  (foreign-lambda* batch ()
    "leveldb::WriteBatch *x = new leveldb::WriteBatch();
     C_return(x);"))

(define delete-batch
  (foreign-lambda* void ((batch b)) "delete b;"))

(define c-leveldb-batch-put
  (foreign-lambda* void ((batch batch) (slice key) (slice value))
    "batch->Put(*key, *value);"))

(define (leveldb-batch-put batch key value)
  (let ([keystr (string->slice key)]
        [valstr (string->slice value)])
    (c-leveldb-batch-put batch keystr valstr)
    (delete-slice keystr)
    (delete-slice valstr)))

(define c-leveldb-batch-del
  (foreign-lambda* void ((batch batch) (slice key))
    "batch->Delete(*key);"))

(define (leveldb-batch-del batch key)
  (let ([keystr (string->slice key)])
    (c-leveldb-batch-del batch keystr)
    (delete-slice keystr)))

(define c-leveldb-write-batch
  (foreign-lambda* void ((DB db) (batch batch) (status s) (bool sync))
    "leveldb::WriteOptions write_options;
     write_options.sync = sync;
     *s = db->Write(write_options, batch);"))

(define (fill-batch batch ops)
  (if (null? ops) batch
    (let* ([op (car ops)]
           [type (car op)]
           [key (cadr op)])
      (cond [(eq? 'put type) (leveldb-batch-put batch key (caddr op))]
            [(eq? 'delete type) (leveldb-batch-del batch key)]
            [else
              (abort (sprintf "Unknown type for batch operation: ~S" type))])
      (fill-batch batch (cdr ops)))))

(define c-open-iterator
  (foreign-lambda* iter ((DB db) (bool fillcache))
    "leveldb::ReadOptions options;
     options.fill_cache = fillcache;
     leveldb::Iterator* x = db->NewIterator(options);
     C_return(x);"))

(define (open-iterator db fillcache)
  (let ([it (c-open-iterator db fillcache)])
    ;; TODO: test if db has been closed
    (set! (slot-value it 'db) db)
    (set! (slot-value db 'iterators) (cons it (slot-value db 'iterators)))
    (set-finalizer! it close-iterator)
    it))

(define iter-next! (foreign-lambda* void ((iter it)) "it->Next();"))
(define iter-prev! (foreign-lambda* void ((iter it)) "it->Prev();"))

(define c-iter-seek
  (foreign-lambda* void ((iter it) (slice start)) "it->Seek(*start);"))

(define (iter-seek! iter key)
  (let ([keyslice (string->slice key)])
    (c-iter-seek iter (string->slice key))
    (delete-slice keyslice)))

(define iter-seek-first!
  (foreign-lambda* void ((iter it)) "it->SeekToFirst();"))

(define iter-seek-last!
  (foreign-lambda* void ((iter it)) "it->SeekToLast();"))

(define iter-valid?
  (foreign-lambda* bool ((iter it)) "C_return(it->Valid());"))

(define c-iter-key
  (foreign-lambda* void ((iter it) (slice ret)) "*ret = it->key();"))

(define (iter-key iter)
  (let* ([ret (make-slice)]
         [void (c-iter-key iter ret)]
         [result (slice->string ret)])
    (delete-slice ret)
    result))

(define c-iter-value
  (foreign-lambda* void ((iter it) (slice ret)) "*ret = it->value();"))

(define (iter-value iter)
  (let* ([ret (make-slice)]
         [void (c-iter-value iter ret)]
         [result (slice->string ret)])
    (delete-slice ret)
    result))

(define c-iter-status
  (foreign-lambda* void ((iter it) (status s))
    "*s = it->status();"))

(define (iter-status iter)
  (let* ([status (make-status)]
         [void (c-iter-status iter status)]
         [ok (status-ok? status)]
         [msg (status-message status)])
    (delete-status status)
    (list ok msg)))

(define c-close-iterator
  (foreign-lambda* void ((iter it)) "delete it;"))

(define (db-closed? db)
  (slot-value db 'closed))

(define (close-iterator it)
  (let ([db (slot-value it 'db)])
    (if (db-closed? db)
      #f ;; DB already closed, not deleting iterator
      (begin
        (set! (slot-value db 'iterators)
          (filter (lambda (x) (eq? it x))
                  (slot-value db 'iterators)))
        (c-close-iterator it)))))

(define (make-stream-value key value)
  (lambda (k it)
    (cond [(and key value) (list (or k (iter-key it)) (iter-value it))]
          [value (iter-value it)]
          [key (or k (iter-key it))]
          [else '()])))

(define (stream-next it end limit make-value start? end? next)
  (let* ([k (iter-key it)]
         [nextlimit (and limit (- limit 1))])
    (if (not (end? end k))
      (let ([head (make-value k it)]
            [void (next it)]
            [tail (make-stream it end nextlimit make-value start? end? next)])
        (if (start? k)
          (cons head tail)
          tail))
      '())))

(define iter-status-ok? car)
(define iter-status-message cadr)

(define (make-stream it end limit make-value start? end? next)
  (lazy-seq
    (cond [(eq? limit 0) '()]
          [(iter-valid? it)
           (stream-next it end limit make-value start? end? next)]
          [else (let ([s (iter-status it)])
                  (if (iter-status-ok? s) '()
                    (abort (iter-status-message s))))])))

(define (init-stream it start reverse)
  (if (eq? start #f)
    ((if reverse iter-seek-last! iter-seek-first!) it)
    (iter-seek! it start)))

(define (stream-start? start reverse)
  (if start
    (if reverse
      (lambda (k) (string<=? k start))
      (lambda (k) (string>=? k start)))
    (lambda (x) #t)))

(define (stream-end? reverse)
  (let ([compare (if reverse string<? string>?)])
    (lambda (end k) (and end (compare k end))))))
