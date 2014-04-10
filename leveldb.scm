(module leveldb
  (
   leveldb-open
   leveldb-close
   leveldb-put
   leveldb-get
   leveldb-make-batch
   leveldb-write-batch
   leveldb-abort-batch
   leveldb-batch-put
   leveldb-batch-delete
   )

(import scheme chicken foreign)
(use coops lolevel)

(foreign-declare "#include <iostream>")
(foreign-declare "#include \"leveldb/db.h\"")
(foreign-declare "#include \"leveldb/write_batch.h\"")


(define-class <db> () ((this '())))
(define-foreign-type DB (instance "leveldb::DB" <db>))


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
   (string-length str)
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
   (string-length str)
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


(define c-leveldb-open
  (foreign-lambda* DB ((c-string loc) (status s) (bool create) (bool noexist))
    "leveldb::DB* db;
     leveldb::Options options;
     options.create_if_missing = create;
     options.error_if_exists = noexist;
     *s = leveldb::DB::Open(options, loc, &db);
     C_return(db);"))

(define (leveldb-open loc #!key (create_if_missing #t) (error_if_exists #f))
  (let* ([status (make-status)]
         [db (c-leveldb-open loc status create_if_missing error_if_exists)])
    (check-status status)
    db))

(define leveldb-close
  (foreign-lambda* void ((DB db)) "delete db;"))

(define c-leveldb-put
  (foreign-lambda* void ((DB db) (slice key) (slice value) (status s))
    "*s = db->Put(leveldb::WriteOptions(), *key, *value);"))

(define (leveldb-put db key value)
  (let ([keystr (string->slice key)]
        [valstr (string->slice value)]
        [status (make-status)])
    (c-leveldb-put db keystr valstr status)
    (delete-slice keystr)
    (delete-slice valstr)
    (check-status status)))

(define c-leveldb-get
  (foreign-lambda* void ((DB db) (slice key) (stdstr ret) (status s))
    "*s = db->Get(leveldb::ReadOptions(), *key, ret);"))

(define (check-status s)
  (if (status-ok? s)
    (begin (delete-status s) #t)
    (let ([msg (status-message s)])
      (delete-status s)
      (abort msg))))

(define (leveldb-get db key)
  (let* ([keystr (string->slice key)]
         [ret (make-stdstr)]
         [status (make-status)]
         [void (c-leveldb-get db keystr ret status)]
         [result (stdstr->string ret)])
    (delete-slice keystr)
    (delete-stdstr ret)
    (check-status status)
    result))


(define-class <batch> () ((this '())))
(define-foreign-type batch (instance "leveldb::WriteBatch" <batch>))

(define leveldb-make-batch
  (foreign-lambda* batch ()
    "leveldb::WriteBatch *x = new leveldb::WriteBatch();
     C_return(x);"))

(define delete-batch
  (foreign-lambda* void ((batch b)) "delete b;"))

(define c-leveldb-batch-put
  (foreign-lambda* void ((batch batch) (slice key) (slice value) (status s))
    "*s = batch->Put(leveldb::WriteOptions(), *key, *value);"))

(define (leveldb-batch-put batch key value)
  (let ([keystr (string->slice key)]
        [valstr (string->slice value)]
        [status (make-status)])
    (c-leveldb-batch-put batch keystr valstr status)
    (delete-slice keystr)
    (delete-slice valstr)
    (check-status status)))

(define c-leveldb-batch-delete
  (foreign-lambda* void ((batch batch) (slice key) (status s))
    "*s = batch->Delete(leveldb::WriteOptions(), *key);"))

(define (leveldb-batch-delete batch key)
  (let ([keystr (string->slice key)]
        [status (make-status)])
    (c-leveldb-batch-delete batch keystr status)
    (delete-slice keystr)
    (check-status status)))

(define c-leveldb-write-batch
  (foreign-lambda* void ((DB db) (batch batch) (status s))
    "*s = db->Write(leveldb::WriteOptions(), batch);"))

(define (leveldb-write-batch db batch)
  (let ([status (make-status)])
    (c-leveldb-write-batch db batch status)
    (delete-batch batch)
    (check-status status)))

(define leveldb-abort-batch delete-batch))
