package com.cj.fintech.lib.batch

import scala.Function.const
case class Ctx[SRC](source: SRC, index: BigInt)

case class BatchContext[SRC, INCOMPLETE]() {

  def reject[A](reason: INCOMPLETE): BatchProcessor[SRC, INCOMPLETE, A] = new BatchProcessor(P.fixed(Left(reason)))
  def pure[A](value: A): BatchProcessor[SRC, INCOMPLETE, A] = new BatchProcessor(P.fixed(Right(value)))
  def source(): BatchProcessor[SRC, INCOMPLETE, SRC] = new BatchProcessor(P.context[SRC, INCOMPLETE]()).map(_.source)
  def index(): BatchProcessor[SRC, INCOMPLETE, BigInt] = new BatchProcessor(P.context[SRC, INCOMPLETE]()).map(_.index)

  def guard(reason: INCOMPLETE)(test: Boolean): BatchProcessor[SRC, INCOMPLETE, Unit]
    = if (test) pure(()) else reject(reason)

}

private[batch] object P {

  type Compute[SRC, INCOMPLETE, A] = Ctx[SRC] => Either[INCOMPLETE, A]

  def fixed[SRC, INCOMPLETE, A](maybeV: Either[INCOMPLETE, A]): Compute[SRC, INCOMPLETE, A] = {
    const(maybeV)
  }

  def context[SRC, INCOMPLETE](): Compute[SRC, INCOMPLETE, Ctx[SRC]] = {
    new Right(_)
  }

  def join[SRC, INCOMPLETE, A](nested: Compute[SRC, INCOMPLETE, Compute[SRC, INCOMPLETE, A]])
    : Compute[SRC, INCOMPLETE, A] = {
      ctx: Ctx[SRC] =>
        nested(ctx).right.flatMap(_(ctx))
  }

  def map[SRC, INCOMPLETE, A, B](compute: Compute[SRC, INCOMPLETE, A])(f: A => B)
    : Compute[SRC, INCOMPLETE, B] = {
        ctx: Ctx[SRC] =>
          compute(ctx).right.map(f)
  }

  def execute[SRC, INCOMPLETE, A](compute: Compute[SRC, INCOMPLETE, A])(batch: Iterable[Ctx[SRC]]): Iterable[Either[INCOMPLETE, A]] = {
    batch.map(compute)
  }

}

class BatchProcessor[SRC, INCOMPLETE, A](
         private[batch] val compute: P.Compute[SRC, INCOMPLETE, A]
     ) {

    type Proc[X] = BatchProcessor[SRC, INCOMPLETE, X]
    type This = Proc[A]
    type Context = Ctx[SRC]
    type Result = Either[INCOMPLETE, A]

    def map[B](f: A => B): Proc[B] =  new BatchProcessor(P.map(compute)(f))

    def flatMap[B](f: A => Proc[B]): Proc[B] =
      new BatchProcessor(P.join(P.map(P.map(this.compute)(f))(_.compute)))

    private def execute(contexts: Iterable[Context]): Iterable[Result] = P.execute(this.compute)(contexts)

    protected def exec_(batch: Iterable[SRC]): Iterable[Item[SRC, Result]] = {
      val contexts = batch.zipWithIndex.map(p => Ctx(p._1, p._2))
      val results = execute(contexts)
      results.zip(contexts).map(p => Item(p._2.index, p._2.source, p._1))
    }

  def run(batch: Iterable[SRC]): Iterable[Item[SRC,A]] = ProcessResult(exec_(batch)).complete
  def exec(batch: Iterable[SRC]): ProcessResult[SRC, INCOMPLETE, A] = ProcessResult(exec_(batch))

}
