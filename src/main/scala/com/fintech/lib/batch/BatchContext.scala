package com.fintech.lib.batch

case class BatchContext[SRC, INCOMPLETE]() {

  def fold[A](initial: A)(f: (A, SRC) => A): BatchProcessor[SRC, INCOMPLETE, A]
      = new Universal({sources: Iterable[SRC] =>
       val summary = sources.foldLeft[A](initial)(f)
       sources.map(Function.const(Right(summary)))
      })
//    = Ag[SRC, INCOMPLETE, A](ss => ss.foldLeft[A](initial)(f))

  def reject[A](reason: INCOMPLETE): BatchProcessor[SRC, INCOMPLETE, A]
    = new Universal(sources => sources.map(Function.const(Left(reason))))
//    = Processor(Function.const(Left(reason)))

  def pure[A](value: A): BatchProcessor[SRC, INCOMPLETE, A]
      = new Universal(sources => sources.map(Function.const(Right(value))))
//    = Processor(Function.const(Right(value)))

  def source(): BatchProcessor[SRC, INCOMPLETE, SRC]
    = new Universal(sources => sources.map(Right(_)))
//    = Processor(Right(_))
}

sealed trait BatchProcessor[SRC, INCOMPLETE, +A] {

  def flatMap[B](f: A => BatchProcessor[SRC, INCOMPLETE, _ <: B]): BatchProcessor[SRC, INCOMPLETE, B] = ???


  def map[B](f: A => B): BatchProcessor[SRC, INCOMPLETE, B] = ???

  protected def exec_(batch: Iterable[SRC]): ProcessResult[SRC, INCOMPLETE, A] = ???

  def run(batch: Iterable[SRC]): Iterable[Item[SRC,A]] = exec_(batch).complete

  def exec(batch: Iterable[SRC]): ProcessResult[SRC, INCOMPLETE, A] = exec_(batch)

}


private[batch] class Universal[SRC, INCOMPLETE, +A](private val r: Iterable[SRC] => Iterable[Either[INCOMPLETE, A]]) extends BatchProcessor[SRC, INCOMPLETE, A] {

  override def map[B](f: A => B): BatchProcessor[SRC, INCOMPLETE, B] = new Universal({sources: Iterable[SRC] => r(sources).map(_.right.map(f))})

  override def flatMap[B](f: A => BatchProcessor[SRC, INCOMPLETE, _ <: B]): BatchProcessor[SRC, INCOMPLETE, B] = {

      val x = {sources: Iterable[SRC] =>
        val maybeAs = r(sources)
        val maybeAsWithsource: Iterable[(Either[INCOMPLETE, A], SRC)] = maybeAs.zip(sources)
        val a: Iterable[(Either[INCOMPLETE, BatchProcessor[SRC, INCOMPLETE, _ <: B]], SRC)]
                = maybeAsWithsource.map(p => (p._1.right.map(f), p._2))
        val b: Iterable[Either[INCOMPLETE, Iterable[Either[INCOMPLETE, B]]]] = a.map(p => p._1.right.map(bp => bp.asInstanceOf[Universal[SRC, INCOMPLETE, _ <: B]].r(Seq(p._2))))
        val c: Iterable[Either[INCOMPLETE, Either[INCOMPLETE, B]]] = b.map(e => e.right.map(ss => ss.head))
        val d: Iterable[Either[INCOMPLETE, B]] = c.map(e => e.joinRight)

        d
      }
      new Universal(x)

  }

  override protected def exec_(batch: Iterable[SRC]): ProcessResult[SRC, INCOMPLETE, A] = {
    ProcessResult(r(batch).zip(batch).zipWithIndex.map(p => Item(p._2, p._1._2,p._1._1 )))
  }

}


private[batch] case class Ag[SRC, INCOMPLETE, +A](aggr: Iterable[SRC] => A) extends BatchProcessor[SRC, INCOMPLETE, A]{

  override def map[B](f: A => B): BatchProcessor[SRC, INCOMPLETE, B] = Ag(sources => f(aggr(sources)))

  override protected def exec_(batch: Iterable[SRC]): ProcessResult[SRC, INCOMPLETE, A] = {
    val a = aggr(batch)
    ProcessResult(batch.zipWithIndex.map(p => Item(p._2, p._1, Right(a))))
  }

}

private[batch] case class Processor[SRC, INCOMPLETE, +A](runLine: SRC => Either[INCOMPLETE, A]) extends BatchProcessor[SRC, INCOMPLETE, A] {

  override def flatMap[B](f: A => BatchProcessor[SRC, INCOMPLETE, _ <: B]): BatchProcessor[SRC, INCOMPLETE, B] = {

    Processor(src => runLine(src).right.map(f).right.flatMap(_.asInstanceOf[Processor[SRC,INCOMPLETE,B]].runLine(src)))

  }

  override def map[B](f: A => B): BatchProcessor[SRC, INCOMPLETE, B] = {
    Processor(src => runLine(src).right.map(f))
  }

  override protected def exec_(batch: Iterable[SRC]): ProcessResult[SRC, INCOMPLETE, A]
  = ProcessResult(batch.zipWithIndex.map(p => Item(p._2, p._1, runLine(p._1))))

}
