;; see: http://wiki.call-cc.org/eggref/4/srfi-41
;; or, even better: http://wiki.call-cc.org/eggref/4/lazy-seq

(use streams)

(define (factorial n)
  (stream-ref (stream-scan * 1 (stream-from 1)) n))

(write (factorial 10))
