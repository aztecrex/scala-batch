package com.cj.fintech.lib.batch

import Function.const
case class Ctx[SRC](source: SRC, index: BigInt, contexts: () => Iterable[Ctx[SRC]])

case class BatchContext[SRC, INCOMPLETE]() {

  type Processor[A] = BatchProcessor[SRC, INCOMPLETE, A]

  def reject[A](reason: INCOMPLETE): BatchProcessor[SRC, INCOMPLETE, A]
    = BatchProcessorY(const(Left(reason)))
//    = BatchProcessor(ctxs => ctxs.map(const(Left(reason))))
//    = new BatchProcessor(const(Left(reason)))

  def pure[A](value: A): BatchProcessor[SRC, INCOMPLETE, A]
    = BatchProcessorY(const(Right(value)))
//      = new BatchProcessor(const(Right(value)))

  def source(): BatchProcessor[SRC, INCOMPLETE, SRC]
    = BatchProcessorY((ctx: Ctx[SRC]) => Right(ctx.source))
//    = new BatchProcessor(ctx => Right(ctx.source))

  def index(): BatchProcessor[SRC, INCOMPLETE, BigInt]
    = BatchProcessorY((ctx: Ctx[SRC]) => Right(ctx.index))
//    = new BatchProcessor(ctx => Right(ctx.index))

  def guard(reason: INCOMPLETE)(test: Boolean): BatchProcessor[SRC, INCOMPLETE, Unit]
    = if (test) pure(()) else reject(reason)

}

//private[batch] trait Step[FROM, TO]  {
//  type From = FROM
//  type To = TO
//  def apply[R](prev: R => FROM): R => TO
//}
//
//private[batch] abstract class AbstractStep[FROM, TO](transform: FROM => TO) extends Step[FROM, TO] {
//  override def apply[R](prev: R => FROM): R => TO = prev.andThen(transform)
//}
//
//private[batch] case class MapStep[A, INCOMPLETE, B](f: A => B)
//    extends AbstractStep[Iterable[Either[INCOMPLETE, A]], Iterable[Either[INCOMPLETE,B]]] (_.map(_.right.map(f)))
//
//private[batch] case class FlatMapStep[A, INCOMPLETE, B](f: A => Either[INCOMPLETE, B])
//  extends AbstractStep[Iterable[Either[INCOMPLETE, A]], Iterable[Either[INCOMPLETE,B]]] (_.map(_.right.map(f).joinRight))


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

trait BatchProcessor[SRC, INCOMPLETE, +A] {

  def map[B](f: A => B): BatchProcessor[SRC, INCOMPLETE, B]
  def flatMap[B](f: A => BatchProcessor[SRC, INCOMPLETE, _ <: B]): BatchProcessor[SRC, INCOMPLETE, B]
  def foldLeft[B](initial: B)(f: (B, A) => B): BatchProcessor[SRC, INCOMPLETE, B]

  protected def exec_(batch: Iterable[SRC]): Iterable[(Either[INCOMPLETE,A], Ctx[SRC])]

  private def result(batch: Iterable[SRC]): ProcessResult[SRC, INCOMPLETE, A] = {
    ProcessResult(exec_(batch).map(p => Item(p._2.index, p._2.source, p._1)))
  }
  //    = ProcessResult(exec_(batch).zip(batch).zipWithIndex.map(pp => Item(pp._2, pp._1._2, pp._1._1)))

  def run(batch: Iterable[SRC]): Iterable[Item[SRC,A]] = result(batch).complete
  def exec(batch: Iterable[SRC]): ProcessResult[SRC, INCOMPLETE, A] = result(batch)

}

private [batch] case class BatchProcessorY[SRC, INCOMPLETE, +A]
  (
    runLine: Ctx[SRC] => Either[INCOMPLETE, A]
  )  extends BatchProcessor[SRC, INCOMPLETE, A] {

  override def flatMap[B](f: A => BatchProcessor[SRC, INCOMPLETE, _ <: B])
    : BatchProcessor[SRC, INCOMPLETE, B] =
    {
        val next = {
          ctx: Ctx[SRC] =>
            val maybeProc= runLine(ctx).right.map(f)
            val maybeProcY = maybeProc.right.map(_.asInstanceOf[BatchProcessorY[SRC, INCOMPLETE, _ <: B]])
            maybeProcY.right.flatMap(_.runLine(ctx))
        }
      BatchProcessorY(next)
    }

  override def map[B](f: A => B): BatchProcessor[SRC, INCOMPLETE, B] = {
    BatchProcessorY(ctx => runLine(ctx).right.map(f))
  }

  private def here(context: Ctx[SRC]): Iterable[Either[INCOMPLETE, A]] = {
    context.contexts().map(runLine)
  }

  override def foldLeft[B](initial: B)(f: (B, A) => B): BatchProcessor[SRC, INCOMPLETE, B] = {
    val next = {
      context: Ctx[SRC] =>
        val maybeA = runLine(context)
        maybeA match {
          case Right(_) => {
            val maybeAs = here(context)
            val as = maybeAs.filter(_.isRight).map(_.right.get)
            val summary = as.foldLeft(initial)(f)
            Right(summary)
          }
          case Left(reason) => Left(reason)
        }
    }
    BatchProcessorY(next)
  }

  private def genContexts(batch: Iterable[SRC]): Iterable[Ctx[SRC]] = {
    batch.zipWithIndex.map(p => Ctx(p._1, p._2, () => genContexts(batch)))
  }

  override protected def exec_(batch: Iterable[SRC]): Iterable[(Either[INCOMPLETE, A], Ctx[SRC])] = {
    val contexts = genContexts(batch)
    contexts.map(runLine).zip(contexts)
  }
}


//private[batch] case class BatchProcessorX[SRC, INCOMPLETE, TRANSIENT, +A](
//       run: TRANSIENT => Either[INCOMPLETE, A],
//       prep: Iterable[Ctx[SRC]] => Iterable[Either[INCOMPLETE, TRANSIENT]])
//        extends BatchProcessor[SRC, INCOMPLETE, A] {
//
//  override def foldLeft[B](initial: B)(f: (B, A) => B): BatchProcessor[SRC, INCOMPLETE, B] = {
//      val prepNext = {
//        contexts: Iterable[Ctx[SRC]] =>
//          val maybeTransients: Iterable[Either[INCOMPLETE, TRANSIENT]] = prep(contexts)
//          val maybeAs: Iterable[Either[INCOMPLETE, A]] = maybeTransients.map(_.right.flatMap(run))
//          val summary = maybeAs.filter(_.isRight).map(_.right.get).foldLeft(initial)(f)
//          maybeAs.map(_.right.map(const(summary)))
//      }
//      BatchProcessorX((b: B) => Right(b), prepNext)
//  }
//
//  override def map[B](f: A => B): BatchProcessor[SRC, INCOMPLETE, B] = {
//    BatchProcessorX[SRC, INCOMPLETE, TRANSIENT,B](run(_).right.map(f), prep)
//  }
//
//  override def flatMap[B](f: A => BatchProcessor[SRC, INCOMPLETE, _ <: B]): BatchProcessor[SRC, INCOMPLETE, B] = {
////    val next = {x: TRANSIENT =>
////      val a: Either[INCOMPLETE, BatchProcessor[SRC, INCOMPLETE, _ <: B]] = run(x).right.map(f)
////
////      CRUX OF THE MATTER RIGHT HERE, CANNOT KNOW WHAT TRANSIENT IS IN THE RETURNED PROCESSORS
////      run(x).right.map(f)
////        .right.map(_.asInstanceOf[BatchProcessorX[SRC, INCOMPLETE, TRANSIENT, B]])
////        .right.map(_.run).right.map(_(x))
////        .joinRight
////    }
////    BatchProcessorX(next, prep)
//    ???
//  }
//
//  override protected def exec_(batch: Iterable[SRC]): Iterable[(Either[INCOMPLETE,A], Ctx[SRC])] = {
//    val contexts = batch.zipWithIndex.map(p => Ctx(p._1, p._2))
//    val prepped: Iterable[Either[INCOMPLETE, TRANSIENT]] = prep(contexts)
//    val runned: Iterable[Either[INCOMPLETE, Either[INCOMPLETE, A]]] = prepped.map(_.right.map(run))
//    val joined: Iterable[Either[INCOMPLETE, A]] = runned.map(_.joinRight)
//    joined.zip(contexts)
////    prepped.right.map(run)
////    run(contexts).zip(contexts)
//  }
//
//}

