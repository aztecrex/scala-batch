package com.fintech.lib.batch

case class BatchContext[SRC, INCOMPLETE]() {

  def fold[A](initial: A)(f: (A, SRC) => A): BatchProcessor[SRC, INCOMPLETE, A]
      = new BatchProcessor({sources: Iterable[SRC] =>
       val summary = sources.foldLeft[A](initial)(f)
       sources.map(Function.const(Right(summary)))
      })

  def reject[A](reason: INCOMPLETE): BatchProcessor[SRC, INCOMPLETE, A]
    = new BatchProcessor(sources => sources.map(Function.const(Left(reason))))

  def pure[A](value: A): BatchProcessor[SRC, INCOMPLETE, A]
      = new BatchProcessor(sources => sources.map(Function.const(Right(value))))

  def source(): BatchProcessor[SRC, INCOMPLETE, SRC]
    = new BatchProcessor(sources => sources.map(Right(_)))
}


class BatchProcessor[SRC, INCOMPLETE, +A](private val r: Iterable[SRC] => Iterable[Either[INCOMPLETE, A]]) {

  def map[B](f: A => B): BatchProcessor[SRC, INCOMPLETE, B] = new BatchProcessor({sources: Iterable[SRC] => r(sources).map(_.right.map(f))})

  def flatMap[B](f: A => BatchProcessor[SRC, INCOMPLETE, _ <: B]): BatchProcessor[SRC, INCOMPLETE, B] = {

      val x = {sources: Iterable[SRC] =>
        val maybeAs = r(sources)
        val maybeAsWithsource: Iterable[(Either[INCOMPLETE, A], SRC)] = maybeAs.zip(sources)
        val a: Iterable[(Either[INCOMPLETE, BatchProcessor[SRC, INCOMPLETE, _ <: B]], SRC)]
                = maybeAsWithsource.map(p => (p._1.right.map(f), p._2))
        val b: Iterable[Either[INCOMPLETE, Iterable[Either[INCOMPLETE, B]]]] = a.map(p => p._1.right.map(bp => bp.r(Seq(p._2))))
        val c: Iterable[Either[INCOMPLETE, Either[INCOMPLETE, B]]] = b.map(e => e.right.map(ss => ss.head))
        val d: Iterable[Either[INCOMPLETE, B]] = c.map(e => e.joinRight)

        d
      }
      new BatchProcessor(x)

  }

  protected def exec_(batch: Iterable[SRC]): ProcessResult[SRC, INCOMPLETE, A] = {
    ProcessResult(r(batch).zip(batch).zipWithIndex.map(p => Item(p._2, p._1._2,p._1._1 )))
  }

    def run(batch: Iterable[SRC]): Iterable[Item[SRC,A]] = exec_(batch).complete

    def exec(batch: Iterable[SRC]): ProcessResult[SRC, INCOMPLETE, A] = exec_(batch)


}

