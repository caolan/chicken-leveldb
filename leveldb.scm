(module leveldb

;; exports
(leveldb
 call-with-db
 open-db
 close-db)

(import scheme chicken foreign)
(use srfi-1 level interfaces lolevel miscmacros lazy-seq)

(import (rename (only scheme string-length)
                (string-length byte-string-length)))

(foreign-declare "#include \"leveldb/c.h\"")

(define leveldb
  (implementation level-api

    (define (level-get db key)
      (or (get-or-false db key)
          (abort
            (make-composite-condition
              (make-property-condition 'exn
                'message "missing key"
                'location 'db-get
                'arguments (list db key))
              (make-property-condition 'level)
              (make-property-condition 'not-found)))))

    (define (level-get/default db key default)
      (or (get-or-false db key) default))

    (define (level-put db key value #!key (sync #f))
      (let ((write-options (make-write-options sync)))
        (call-with-error-pointer
          (cut leveldb-put
               db
               write-options
               key
               (string-length key)
               value
               (string-length value)
               <>))))

    (define (level-delete db key #!key (sync #f))
      (let ((write-options (make-write-options sync)))
        (call-with-error-pointer
          (cut leveldb-delete
               db
               write-options
               key
               (string-length key)
               <>))))

    (define (level-batch db ops #!key (sync #f))
      (let ((batch (list->batch ops))
            (write-options (make-write-options sync)))
        (call-with-error-pointer
          (cut leveldb-write
               db
               write-options
               batch
               <>))))

    (define (level-stream db #!key start end limit reverse
                          (key #t) (value #t) fillcache)

      (let* ((read-options (make-read-options))
             (iter (make-iterator db read-options)))
        (init-stream iter start reverse)
        (make-stream iter end limit
                     (make-stream-value key value)
                     (stream-start? start reverse)
                     (stream-end? reverse)
                     (if reverse
                       iterator-prev
                       iterator-next))))))

(define (get-or-false db key)
  (let ((read-options (make-read-options)))
    (let-location ((size size_t))
      (and-let* ((value (call-with-error-pointer
                          (cut leveldb-get
                               db
                               read-options
                               key
                               (string-length key)
                               (location size)
                               <>))))
        (begin0
          (pointer->string value size)
          (free value))))))

(define (leveldb-condition err)
  (make-composite-condition
    (make-property-condition 'exn 'message err)
    (make-property-condition 'leveldb)))

(define (call-with-error-pointer thunk)
  (let-location ((err c-string* #f))
    (begin0
      (thunk (location err))
      (and-let* ((err err))
        (abort (leveldb-condition err))))))

(define copy-to-string
  (foreign-lambda* void ((scheme-pointer str) (c-pointer p) (size_t size))
    "memcpy(str, p, size);"))

(define (pointer->string p size)
  (let ((str (make-string size)))
    (copy-to-string str p size)
    str))

(define make-leveldb-options
  (foreign-lambda* (c-pointer (struct leveldb_options_t))
    ((bool create) (bool exists))
    "leveldb_options_t *options = leveldb_options_create();
     leveldb_options_set_create_if_missing(options, create);
     leveldb_options_set_error_if_exists(options, !exists);
     C_return(options);"))

(define make-write-options
  (foreign-lambda* (c-pointer (struct leveldb_writeoptions_t))
    ((bool sync))
    "leveldb_writeoptions_t *woptions = leveldb_writeoptions_create();
     leveldb_writeoptions_set_sync(woptions, sync);
     C_return(woptions);"))

(define make-read-options
  (foreign-lambda* (c-pointer (struct leveldb_readoptions_t))
    ()
    "leveldb_readoptions_t *roptions = leveldb_readoptions_create();
     C_return(roptions);"))

(define leveldb-open
  (foreign-lambda (c-pointer (struct leveldb_t)) "leveldb_open"
    (c-pointer (struct leveldb_options_t))
    c-string
    (c-pointer c-string)))

(define leveldb-put
  (foreign-lambda void "leveldb_put"
    (c-pointer (struct leveldb_t))
    (c-pointer (struct leveldb_writeoptions_t))
    scheme-pointer
    size_t
    scheme-pointer
    size_t
    (c-pointer c-string)))

(define leveldb-get
  (foreign-lambda c-pointer "leveldb_get"
    (c-pointer (struct leveldb_t))
    (c-pointer (struct leveldb_readoptions_t))
    scheme-pointer
    size_t
    (c-pointer size_t)
    (c-pointer c-string)))

(define leveldb-delete
  (foreign-lambda void "leveldb_delete"
    (c-pointer (struct leveldb_t))
    (c-pointer (struct leveldb_writeoptions_t))
    scheme-pointer
    size_t
    (c-pointer c-string)))

(define make-batch
  (foreign-lambda (c-pointer (struct leveldb_writebatch_t))
    "leveldb_writebatch_create"))

(define batch-put
  (foreign-lambda void "leveldb_writebatch_put"
    (c-pointer (struct leveldb_writebatch_t))
    scheme-pointer
    size_t
    scheme-pointer
    size_t))

(define batch-delete
  (foreign-lambda void "leveldb_writebatch_delete"
    (c-pointer (struct leveldb_writebatch_t))
    scheme-pointer
    size_t))

(define leveldb-write
  (foreign-lambda void "leveldb_write"
    (c-pointer (struct leveldb_t))
    (c-pointer (struct leveldb_writeoptions_t))
    (c-pointer (struct leveldb_writebatch_t))
    (c-pointer c-string)))

(define (list->batch ops)
  (fold
    (lambda (op batch)
      (let* ((type (first op))
             (key (second op)))
        (case type
          ((put)
           (let ((value (third op)))
             (batch-put
               batch
               key (string-length key)
               value (string-length value))))
          ((delete)
           (batch-delete
             batch key (string-length key)))
          (else
            (abort
              (leveldb-condition
                (sprintf "Unknown type for batch operation: ~S" type)))))
        batch))
    (make-batch)
    ops))

(define make-iterator
  (foreign-lambda (c-pointer (struct leveldb_iterator_t))
    "leveldb_create_iterator"
    (c-pointer (struct leveldb_t))
    (c-pointer (struct leveldb_readoptions_t))))

(define iterator-seek-to-first
  (foreign-lambda void "leveldb_iter_seek_to_first"
    (c-pointer (struct leveldb_iterator_t))))

(define iterator-seek-to-last
  (foreign-lambda void "leveldb_iter_seek_to_last"
    (c-pointer (struct leveldb_iterator_t))))

(define iterator-seek
  (foreign-lambda void "leveldb_iter_seek"
    (c-pointer (struct leveldb_iterator_t))
    scheme-pointer
    size_t))

(define iterator-next
  (foreign-lambda void "leveldb_iter_next"
    (c-pointer (struct leveldb_iterator_t))))

(define iterator-prev
  (foreign-lambda void "leveldb_iter_prev"
    (c-pointer (struct leveldb_iterator_t))))

(define iterator-key
  (foreign-lambda c-pointer "leveldb_iter_key"
    (c-pointer (struct leveldb_iterator_t))
    (c-pointer size_t)))

(define (iterator-key-string iter)
  (let-location ((size size_t))
    (and-let* ((key (iterator-key iter (location size))))
      (pointer->string key size))))

(define iterator-value
  (foreign-lambda c-pointer "leveldb_iter_value"
    (c-pointer (struct leveldb_iterator_t))
    (c-pointer size_t)))

(define (iterator-value-string iter)
  (let-location ((size size_t))
    (and-let* ((value (iterator-value iter (location size))))
      (pointer->string value size))))

(define (iterator-check-error iter)
  (call-with-error-pointer
    (cut (foreign-lambda void "leveldb_iter_get_error"
           (c-pointer (struct leveldb_iterator_t))
           (c-pointer c-string))
         iter <>)))

(define iterator-valid?
  (foreign-lambda bool "leveldb_iter_valid"
    (c-pointer (struct leveldb_iterator_t))))

(define (init-stream iter start reverse)
  (if (eq? start #f)
    (if reverse
      (iterator-seek-to-last iter)
      (iterator-seek-to-first iter))
    (iterator-seek iter start (string-length start))))

(define (stream-start? start reverse)
  (if start
    (if reverse
      (lambda (k) (string<=? k start))
      (lambda (k) (string>=? k start)))
    (lambda (x) #t)))

(define (stream-end? reverse)
  (let ((compare (if reverse string<? string>?)))
    (lambda (end k) (and end (compare k end)))))

(define (make-stream iter end limit make-value start? end? next)
  (lazy-seq
    (cond
      ((eq? limit 0) '())
      ((iterator-valid? iter)
       (stream-next iter end limit make-value start? end? next))
      (else
        (iterator-check-error iter)
        '()))))

(define (make-stream-value key value)
  (cond
    ((and key value)
     (lambda (k iter)
       (cons k (iterator-value-string iter))))
    (key
      (lambda (k iter)
        k))
    (value
      (lambda (k iter)
        (iterator-value-string iter)))
    (else
      (abort
        (leveldb-condition
          "a stream must return keys, values, or both")))))

(define (stream-next iter end limit make-value start? end? next)
  (let* ((k (iterator-key-string iter))
         (l (and limit (- limit 1))))
    (if (not (end? end k))
      (let ((head (make-value k iter))
            (void (next iter))
            (tail (make-stream iter end l make-value start? end? next)))
        (if (start? k)
          (cons head tail)
          tail))
      '())))

(define (open-db loc #!key (create #t) (exists #t))
  (let ((options (make-leveldb-options create exists)))
    (make-level 'leveldb leveldb
      (call-with-error-pointer
        (cut leveldb-open options loc <>)))))

(define leveldb-close
  (foreign-lambda void "leveldb_close" (c-pointer (struct leveldb_t))))

(define (close-db db)
  (and-let* ((resource (level-resource db)))
    (leveldb-close resource)
    (level-resource-set! db #f)))

(define (call-with-db loc proc #!key (create #t) (exists #t))
  (let ((db (open-db loc create: create exists: exists)))
    (dynamic-wind (lambda () #f)
                  (lambda () (proc db))
                  (lambda () (close-db db)))))

)
