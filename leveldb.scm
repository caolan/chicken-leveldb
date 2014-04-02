(module leveldb
        (leveldb-open leveldb-put leveldb-get)

(import scheme chicken foreign)
(use coops)

(foreign-declare "#include <iostream>")
(foreign-declare "#include \"leveldb/db.h\"")

(define-class <db> ()
              ((this '())))


(define leveldb-open
  (foreign-lambda* (instance "leveldb::DB" <db>) ((c-string loc))
                 "leveldb::DB* db;
                  leveldb::Options options;
                  leveldb::Status status;
                  options.create_if_missing = true;
                  status = leveldb::DB::Open(options, loc, &db);
                  C_return(db);"))

(define c-leveldb-put
  (foreign-lambda* int (((instance "leveldb::DB" <db>) db) (scheme-pointer keydata) (integer keysize) (scheme-pointer valuedata) (integer valuesize))
                 "leveldb::Status status;
                  std::string key = std::string((const char*)keydata, keysize);
                  std::string value = std::string((const char*)valuedata, valuesize);
                  status = db->Put(leveldb::WriteOptions(), key, value);
                  C_return(0);"))

(define (leveldb-put db key value)
  (c-leveldb-put db key (string-length key) value (string-length value)))

(define c-leveldb-get
  (foreign-lambda* c-string (((instance "leveldb::DB" <db>) db) (scheme-pointer keydata) (integer keysize))
                 "leveldb::Status status;
                  std::string ret;
                  std::string key = std::string((const char*)keydata, keysize);
                  status = db->Get(leveldb::ReadOptions(), key, &ret);
                  C_return(ret.c_str());"))

(define (leveldb-get db key)
  (c-leveldb-get db key (string-length key))))
