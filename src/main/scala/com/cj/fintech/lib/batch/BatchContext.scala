package com.cj.fintech.lib.batch

import Function.const
case class Ctx[SRC](source: SRC, index: BigInt)

case class BatchContext[SRC, INCOMPLETE]() {

  type Processor[A] = BatchProcessor[SRC, INCOMPLETE, A]

  def reject[A](reason: INCOMPLETE): BatchProcessor[SRC, INCOMPLETE, A]
    = new BatchProcessor(const(Left(reason)), LineMap)

  def pure[A](value: A): BatchProcessor[SRC, INCOMPLETE, A]
      = new BatchProcessor(const(Right(value)), LineMap)

  def source(): BatchProcessor[SRC, INCOMPLETE, SRC]
    = new BatchProcessor(ctx => Right(ctx.source), LineMap)

  def index(): BatchProcessor[SRC, INCOMPLETE, BigInt]
    = new BatchProcessor(ctx => Right(ctx.index), LineMap)

  def guard(reason: INCOMPLETE)(test: Boolean): BatchProcessor[SRC, INCOMPLETE, Unit]
    = if (test) pure(()) else reject(reason)

}

private[batch] trait BatchRunner {
  type LineRunner[SRC, INCOMPLETE, A] = Ctx[SRC] => Either[INCOMPLETE, A]
  def apply[SRC, INCOMPLETE, A](runLine: LineRunner[SRC, INCOMPLETE, A])(batch: Iterable[Ctx[SRC]]): Iterable[Either[INCOMPLETE, A]]
}

private[batch] object LineMap extends BatchRunner {
  override def apply[SRC, INCOMPLETE, A]
  (runLine: LineRunner[SRC, INCOMPLETE, A])
  (batch: Iterable[Ctx[SRC]])
    : Iterable[Either[INCOMPLETE, A]] = {
    batch.map(runLine)
  }
}

//private[batch] case class Fold[X, B](initial: B, f: (B, X) => B) extends BatchRunner {
//  override def apply[SRC, INCOMPLETE, A]
//  (runLine: LineRunner[SRC, INCOMPLETE, A])
//  (batch: Iterable[Ctx[SRC]]): Iterable[Either[INCOMPLETE, A]] = {
//    val summary = batch.map(runLine).foldLeft(initial)(f)
//  }
//}

class BatchProcessor[SRC, INCOMPLETE, +A](
      private val runLine: Ctx[SRC] => Either[INCOMPLETE, A],
      private val runner: BatchRunner) {

  def fold[B](initial: B)(f: (B, A) => B): BatchProcessor[SRC, INCOMPLETE, B] = {
//    val aggregate Aggregate
//    val x = {ctx: Ctx[SRC] =>
//      val summary = initial
//      val maybeA = runLine(ctx)
//      maybeA.right.map(Function.const(summary))
//    }
//    new BatchProcessor(x)
    ???
  }

  def map[B](f: A => B): BatchProcessor[SRC, INCOMPLETE, B]
    = new BatchProcessor({ ctx: Ctx[SRC] => runLine(ctx).right.map(f)}, LineMap)

  def flatMap[B](f: A => BatchProcessor[SRC, INCOMPLETE, _ <: B]): BatchProcessor[SRC, INCOMPLETE, B]
    = new BatchProcessor({ ctx: Ctx[SRC] => runLine(ctx).right.map(f).right.map(_.runLine(ctx)).joinRight}, LineMap)

  protected def exec_(batch: Iterable[SRC]): Iterable[Either[INCOMPLETE,A]] = {
    runner(runLine)(batch.zipWithIndex.map(p => Ctx(p._1, p._2)))
  }

  private def result(batch: Iterable[SRC]): ProcessResult[SRC, INCOMPLETE, A]
    = ProcessResult(exec_(batch).zip(batch).zipWithIndex.map(pp => Item(pp._2, pp._1._2, pp._1._1)))

  def run(batch: Iterable[SRC]): Iterable[Item[SRC,A]] = result(batch).complete
  def exec(batch: Iterable[SRC]): ProcessResult[SRC, INCOMPLETE, A] = result(batch)
}

