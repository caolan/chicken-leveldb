;; see: http://wiki.call-cc.org/eggref/4/srfi-41

(use streams)

(define (factorial n)
  (stream-ref (stream-scan * 1 (stream-from 1)) n))

(write (factorial 10))
