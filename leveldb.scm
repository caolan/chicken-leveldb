(module leveldb
  (leveldb-open leveldb-put leveldb-get)

(import scheme chicken foreign)
(use coops lolevel)

(foreign-declare "#include <iostream>")
(foreign-declare "#include \"leveldb/db.h\"")


(define-class <db> () ((this '())))
(define-foreign-type DB (instance "leveldb::DB" <db>))

(define-class <std-string> () ((this '())))
(define-foreign-type std-string (instance "std::string" <std-string>))


(define str-data
  (foreign-lambda* (c-pointer unsigned-char) ((std-string str))
    "C_return(str->data());"))

(define str-size
  (foreign-lambda* integer ((std-string  str))
    "C_return(str->size());"))

(define leveldb-open
  (foreign-lambda* DB ((c-string loc))
    "leveldb::DB* db;
     leveldb::Options options;
     leveldb::Status status;
     options.create_if_missing = true;
     status = leveldb::DB::Open(options, loc, &db);
     C_return(db);"))

(define c-leveldb-put
  (foreign-lambda* int
    ((DB db)
     (scheme-pointer keydata)
     (integer keysize)
     (scheme-pointer valuedata)
     (integer valuesize))
    "leveldb::Status status;
     std::string key = std::string((const char*)keydata, keysize);
     std::string value = std::string((const char*)valuedata, valuesize);
     status = db->Put(leveldb::WriteOptions(), key, value);
     C_return(0);"))

(define (leveldb-put db key value)
  (c-leveldb-put db key (string-length key) value (string-length value)))

(define c-leveldb-get
  (foreign-lambda* std-string
    ((DB db) (scheme-pointer keydata) (integer keysize))
    "leveldb::Status status;
     std::string *ret = new std::string();
     std::string key = std::string((const char*)keydata, keysize);
     status = db->Get(leveldb::ReadOptions(), key, ret);
     C_return(ret);"))

(define c-delete-ret
  (foreign-lambda* void ((std-string ret)) "delete ret;"))

(define (leveldb-get db key)
  (let* ([keylen (string-length key)]
         [ret (c-leveldb-get db key keylen)]
         [retsize (str-size ret)]
         [retdata (str-data ret)]
         [result (make-string retsize)])
    (move-memory! retdata result retsize)
    (c-delete-ret ret)
    result)))
