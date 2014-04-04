(module leveldb
  (leveldb-open leveldb-put leveldb-get)

(import scheme chicken foreign)
(use coops lolevel)

(foreign-declare "#include <iostream>")
(foreign-declare "#include \"leveldb/db.h\"")


(define-class <db> () ((this '())))
(define-foreign-type DB (instance "leveldb::DB" <db>))


(define-class <stdstr> () ((this '())))
(define-foreign-type stdstr (instance "std::string" <stdstr>))

(define stdstr-data
  (foreign-lambda* (c-pointer unsigned-char) ((stdstr str))
    "C_return(str->data());"))

(define stdstr-size
  (foreign-lambda* integer ((stdstr  str))
    "C_return(str->size());"))

(define stdstr-delete
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

(define status-delete
  (foreign-lambda* void ((status s)) "delete s;"))


(define leveldb-open
  (foreign-lambda* DB ((c-string loc))
    "leveldb::DB* db;
     leveldb::Options options;
     leveldb::Status status;
     options.create_if_missing = true;
     status = leveldb::DB::Open(options, loc, &db);
     C_return(db);"))

(define c-leveldb-put
  (foreign-lambda* int ((DB db) (stdstr key) (stdstr value))
    "leveldb::Status status;
     status = db->Put(leveldb::WriteOptions(), *key, *value);
     C_return(0);"))

(define (leveldb-put db key value)
  (c-leveldb-put db (string->stdstr key) (string->stdstr value)))

(define c-leveldb-get
  (foreign-lambda* int ((DB db) (stdstr key) (stdstr ret) (status s))
    "*s = db->Get(leveldb::ReadOptions(), *key, ret);
     C_return(0);"))

(define (check-status s)
  (if (status-ok? s)
    (begin (status-delete s) #t)
    (let ([msg (status-message s)])
      (status-delete s)
      (abort msg))))

(define (leveldb-get db key)
  (let* ([keystr (string->stdstr key)]
         [ret (make-stdstr)]
         [status (make-status)]
         [void (c-leveldb-get db keystr ret status)]
         [result (stdstr->string ret)])
    (stdstr-delete keystr)
    (stdstr-delete ret)
    (check-status status)
    result)))
