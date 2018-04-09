package com.cj.fintech.lib.batch

import Function.const
case class Ctx[SRC](source: SRC, index: BigInt)

case class BatchContext[SRC, INCOMPLETE]() {

  type Processor[A] = BatchProcessor[SRC, INCOMPLETE, A]

  def reject[A](reason: INCOMPLETE): Processor[A]
    = BatchProcessor(ctxs => ctxs.map(const(Left(reason))))
//    = new BatchProcessor(const(Left(reason)))

  def pure[A](value: A): Processor[A]
    = BatchProcessor(ctxs => ctxs.map(const(Right(value))))
//      = new BatchProcessor(const(Right(value)))

  def source(): Processor[SRC]
    = BatchProcessor(ctxs => ctxs.map(ctx => Right(ctx.source)))
//    = new BatchProcessor(ctx => Right(ctx.source))

  def index(): Processor[BigInt]
    = BatchProcessor(ctxs => ctxs.map(ctx => Right(ctx.index)))
//    = new BatchProcessor(ctx => Right(ctx.index))

  def guard(reason: INCOMPLETE)(test: Boolean): Processor[Unit]
    = if (test) pure(()) else reject(reason)

}

private[batch] trait Step[FROM, TO]  {
  type From = FROM
  type To = TO
  def apply[R](prev: R => FROM): R => TO
}

private[batch] abstract class AbstractStep[FROM, TO](transform: FROM => TO) extends Step[FROM, TO] {
  override def apply[R](prev: R => FROM): R => TO = prev.andThen(transform)
}

private[batch] case class MapStep[A, INCOMPLETE, B](f: A => B)
    extends AbstractStep[Iterable[Either[INCOMPLETE, A]], Iterable[Either[INCOMPLETE,B]]] (_.map(_.right.map(f)))

private[batch] case class FlatMapStep[A, INCOMPLETE, B](f: A => Either[INCOMPLETE, B])
  extends AbstractStep[Iterable[Either[INCOMPLETE, A]], Iterable[Either[INCOMPLETE,B]]] (_.map(_.right.map(f).joinRight))


//private[batch] case class BindStep[A, INCOMPLETE, B](f: A => Either[INCOMPLETE, B]) extends Step[Either[INCOMPLETE, A], INCOMPLETE, B] {
//  override def apply(prev: From): To = prev.map {
//      ea: Either[INCOMPLETE, A] => ea.right.map(f).joinRight
//  }
//}
//
//private[batch] case class InitStep[SRC, INCOMPLETE]() extends Step[SRC, INCOMPLETE, Ctx[SRC]] {
//  override def apply(prev: From): To = {
//    prev.zipWithIndex.map(p => Right(Ctx(p._1, p._2)))
//  }
//}
//


//private[batch] trait BatchRunner {
//  type LineRunner[SRC, INCOMPLETE, A] = Ctx[SRC] => Either[INCOMPLETE, A]
//  def apply[SRC, INCOMPLETE, A](runLine: LineRunner[SRC, INCOMPLETE, A])(batch: Iterable[Ctx[SRC]]): Iterable[Either[INCOMPLETE, A]]
//}
//
//private[batch] object LineMap extends BatchRunner {
//  override def apply[SRC, INCOMPLETE, A]
//  (runLine: LineRunner[SRC, INCOMPLETE, A])
//  (batch: Iterable[Ctx[SRC]])
//    : Iterable[Either[INCOMPLETE, A]] = {
//    batch.map(runLine)
//  }
//}

//private[batch] case class Fold[X, B](initial: B, f: (B, X) => B) extends BatchRunner {
//  override def apply[SRC, INCOMPLETE, A]
//  (runLine: LineRunner[SRC, INCOMPLETE, A])
//  (batch: Iterable[Ctx[SRC]]): Iterable[Either[INCOMPLETE, A]] = {
//    val summary = batch.map(runLine).foldLeft(initial)(f)
//  }
//}

case class BatchProcessor[SRC, INCOMPLETE, +A](run: Iterable[Ctx[SRC]] => Iterable[Either[INCOMPLETE, A]]) {

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

  def map[B](f: A => B): BatchProcessor[SRC, INCOMPLETE, B] = {
    BatchProcessor(MapStep[A, INCOMPLETE, B](f).apply(run))
  }

  def flatMap[B](f: A => BatchProcessor[SRC, INCOMPLETE, _ <: B]): BatchProcessor[SRC, INCOMPLETE, B] = {
    val next = {
        contexts: Iterable[Ctx[SRC]] =>
        val maybeAs: Iterable[Either[INCOMPLETE, A]] = run(contexts)
        val maybeProcessors = maybeAs.map(_.right.map(f))
        val maybeRuns = maybeProcessors.map(_.right.map(_.run))
        val maybeRunsWithContexts: Iterable[(Either[INCOMPLETE, Iterable[Ctx[SRC]] => Iterable[Either[INCOMPLETE, B]]], Ctx[SRC])] = maybeRuns.zip(contexts)
        maybeRunsWithContexts.map {
          p: ((Either[INCOMPLETE, Iterable[Ctx[SRC]] => Iterable[Either[INCOMPLETE, B]]], Ctx[SRC])) =>
            val maybeRun = p._1
            val ctx = p._2
            val maybeSingletonB = maybeRun.right.map(run => run(Seq(ctx)))
            val b: Either[INCOMPLETE, B] = maybeSingletonB match {
              case Right(is) => is.head
              case Left(reason) => Left(reason)
            }
            b
        }
    }
    BatchProcessor(next)
  }

  protected def exec_(batch: Iterable[SRC]): Iterable[(Either[INCOMPLETE,A], Ctx[SRC])] = {
    val contexts = batch.zipWithIndex.map(p => Ctx(p._1, p._2))
    run(contexts).zip(contexts)
  }

  private def result(batch: Iterable[SRC]): ProcessResult[SRC, INCOMPLETE, A] = {
    ProcessResult(exec_(batch).map(p => Item(p._2.index, p._2.source, p._1)))
  }
//    = ProcessResult(exec_(batch).zip(batch).zipWithIndex.map(pp => Item(pp._2, pp._1._2, pp._1._1)))

  def run(batch: Iterable[SRC]): Iterable[Item[SRC,A]] = result(batch).complete
  def exec(batch: Iterable[SRC]): ProcessResult[SRC, INCOMPLETE, A] = result(batch)
}

