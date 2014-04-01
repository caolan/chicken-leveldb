(module leveldb (test)

(import scheme chicken foreign)

(foreign-declare "#include <iostream>")
(foreign-declare "#include \"leveldb/db.h\"")

(define test
  (foreign-lambda* int ()
                 "leveldb::DB* db;
                  leveldb::Options options;
                  leveldb::Status status;

                  options.create_if_missing = true;

                  status = leveldb::DB::Open(options, \"./testdb\", &db);

                  std::string key = \"abc\";
                  std::string value = \"123\";
                  std::string ret;
                  std::cout << value;
                  status = db->Put(leveldb::WriteOptions(), key, value);
                  status = db->Get(leveldb::ReadOptions(), key, &ret);
                  std::cout << ret;

                  C_return(0);")))
