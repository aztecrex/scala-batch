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

  def fold[B](initial: B)(f: (B, A) => B): BatchProcessor[SRC, INCOMPLETE, B] = {
    new BatchProcessor({sources: Iterable[SRC] =>
      val unpacked = r(sources)
      val summary = unpacked.filter(_.isRight).map(_.right.get).foldLeft[B](initial)(f)
      unpacked.map(x => x.right.map(Function.const(summary)))
    })
  }

  def map[B](f: A => B): BatchProcessor[SRC, INCOMPLETE, B]
    = new BatchProcessor({sources: Iterable[SRC] => r(sources).map(_.right.map(f))})

  def flatMap[B](f: A => BatchProcessor[SRC, INCOMPLETE, _ <: B]): BatchProcessor[SRC, INCOMPLETE, B] = {
      new BatchProcessor({sources: Iterable[SRC] =>
        val unpacked = r(sources).zip(sources)
        val applied = unpacked.map(p => (p._1.right.map(f), p._2))
        val exploded = applied.map(p => p._1.right.map(bp => bp.r(Seq(p._2)).head))
        val joined = exploded.map(e => e.joinRight)

        joined
      })
  }

  protected def exec_(batch: Iterable[SRC]): ProcessResult[SRC, INCOMPLETE, A] = {
    ProcessResult(r(batch).zip(batch).zipWithIndex.map(p => Item(p._2, p._1._2,p._1._1 )))
  }
  def run(batch: Iterable[SRC]): Iterable[Item[SRC,A]] = exec_(batch).complete
  def exec(batch: Iterable[SRC]): ProcessResult[SRC, INCOMPLETE, A] = exec_(batch)
}

