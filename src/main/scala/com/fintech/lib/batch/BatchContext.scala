package com.fintech.lib.batch

case class BatchContext[SRC, INCOMPLETE]() {

  def fold[A](initial: A)(f: (A, SRC) => A): BatchProcessor[SRC, INCOMPLETE, A]
    = Ag[SRC, INCOMPLETE, A](ss => ss.foldLeft[A](initial)(f))

  def reject[A](reason: INCOMPLETE): BatchProcessor[SRC, INCOMPLETE, A]
    = Processor(Function.const(Left(reason)))

  def pure[A](value: A): BatchProcessor[SRC, INCOMPLETE, A]
    = Processor(Function.const(Right(value)))

  def source(): BatchProcessor[SRC, INCOMPLETE, SRC]
    = Processor(Right(_))
}

sealed trait BatchProcessor[SRC, INCOMPLETE, +A] {

  def flatMap[B](f: A => BatchProcessor[SRC, INCOMPLETE, _ <: B]): BatchProcessor[SRC, INCOMPLETE, B] = ???


  def map[B](f: A => B): BatchProcessor[SRC, INCOMPLETE, B] = ???

  protected def exec_(batch: Iterable[SRC]): ProcessResult[SRC, INCOMPLETE, A] = ???

  def run(batch: Iterable[SRC]): Iterable[Item[SRC,A]] = exec_(batch).complete

  def exec(batch: Iterable[SRC]): ProcessResult[SRC, INCOMPLETE, A] = exec_(batch)

}

private[batch] case class Ag[SRC, INCOMPLETE, +A](f: Iterable[SRC] => A) extends BatchProcessor[SRC, INCOMPLETE, A]{

  override protected def exec_(batch: Iterable[SRC]): ProcessResult[SRC, INCOMPLETE, A] = {
    val a = f(batch)
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
