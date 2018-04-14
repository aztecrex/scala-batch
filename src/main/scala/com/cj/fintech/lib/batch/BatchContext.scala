package com.cj.fintech.lib.batch

import Function.const
case class Ctx[SRC](source: SRC, index: BigInt)

private[batch] object T {
  type Contexts[SRC] = Iterable[Ctx[SRC]]
  type Compute[SRC, INCOMPLETE, C, A] = (Contexts[SRC], Ctx[SRC]) => Either[INCOMPLETE, A]
}

case class BatchContext[SRC, INCOMPLETE]() {


  private def bconst[A](v: Either[INCOMPLETE, A]): T.Compute[SRC, INCOMPLETE, Ctx[SRC], A] = (_, _) => v
  private def bsource: T.Compute[SRC, INCOMPLETE, Ctx[SRC], SRC] = (_, ctx) => Right(ctx.source)
  private def bindex: T.Compute[SRC, INCOMPLETE, Ctx[SRC], BigInt] = (_, ctx) => Right(ctx.index)

  private def processor[A](compute: T.Compute[SRC, INCOMPLETE, Ctx[SRC], A]): BatchProcessor[SRC, INCOMPLETE, A]
    = BatchProcessorY(compute)

  def reject[A](reason: INCOMPLETE): BatchProcessor[SRC, INCOMPLETE, A] = processor(bconst(Left(reason)))
  def pure[A](value: A): BatchProcessor[SRC, INCOMPLETE, A] = processor(bconst(Right(value)))
  def source(): BatchProcessor[SRC, INCOMPLETE, SRC] = processor(bsource)
  def index(): BatchProcessor[SRC, INCOMPLETE, BigInt] = processor(bindex)

  def sum(value: BigInt): BatchProcessor[SRC, INCOMPLETE, BigInt]
  = BatchProcessorY(null)


  def guard(reason: INCOMPLETE)(test: Boolean): BatchProcessor[SRC, INCOMPLETE, Unit]
    = if (test) pure(()) else reject(reason)

}


trait BatchProcessor[SRC, INCOMPLETE, +A] {

  def map[B](f: A => B): BatchProcessor[SRC, INCOMPLETE, B]
  def flatMap[B](f: A => BatchProcessor[SRC, INCOMPLETE, _ <: B]): BatchProcessor[SRC, INCOMPLETE, B]

  protected def exec_(batch: Iterable[SRC]): Iterable[(Either[INCOMPLETE,A], Ctx[SRC])]

  private def result(batch: Iterable[SRC]): ProcessResult[SRC, INCOMPLETE, A] = {
    ProcessResult(exec_(batch).map(p => Item(p._2.index, p._2.source, p._1)))
  }
  //    = ProcessResult(exec_(batch).zip(batch).zipWithIndex.map(pp => Item(pp._2, pp._1._2, pp._1._1)))

  def run(batch: Iterable[SRC]): Iterable[Item[SRC,A]] = result(batch).complete
  def exec(batch: Iterable[SRC]): ProcessResult[SRC, INCOMPLETE, A] = result(batch)

}

private [batch] case class BatchProcessorY[SRC, INCOMPLETE, TRANSIENT, +A]
  (compute: T.Compute[SRC, INCOMPLETE, TRANSIENT, A])
  extends BatchProcessor[SRC, INCOMPLETE, A] {

  override def map[B](f: A => B): BatchProcessor[SRC, INCOMPLETE, B]
    = BatchProcessorY((batch: T.Contexts[SRC], source: Ctx[SRC]) => compute(batch, source).right.map(f))


  override def flatMap[B](f: A => BatchProcessor[SRC, INCOMPLETE, _ <: B])
    : BatchProcessor[SRC, INCOMPLETE, B] =
    {
      val next = {
        (batch: T.Contexts[SRC], source: Ctx[SRC]) =>
          val maybeProc = compute(batch, source).right.map(f)
          val maybeProcY = maybeProc.right.map(_.asInstanceOf[BatchProcessorY[SRC, INCOMPLETE, TRANSIENT, B]])
          maybeProcY.right.flatMap(_.compute(batch, source))
      }
      BatchProcessorY(next)
    }

  override protected def exec_(batch: Iterable[SRC]): Iterable[(Either[INCOMPLETE, A], Ctx[SRC])] = {
      val contexts = batch.zipWithIndex.map(p => Ctx(p._1, p._2))
      contexts.map(ctx => compute(contexts, ctx)).zip(contexts)
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

