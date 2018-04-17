package com.cj.fintech.lib.batch

import scala.Function.const
case class Ctx[SRC](source: SRC, index: BigInt)

case class BatchContext[SRC, INCOMPLETE]() {

  def reject[A](reason: INCOMPLETE): BatchProcessor[SRC, INCOMPLETE, A] = new BatchProcessor(P.fixed(Left(reason)))
  def pure[A](value: A): BatchProcessor[SRC, INCOMPLETE, A] = new BatchProcessor(P.fixed(Right(value)))
  def source(): BatchProcessor[SRC, INCOMPLETE, SRC] = new BatchProcessor(P.context[SRC, INCOMPLETE]()).map(_.source)
  def index(): BatchProcessor[SRC, INCOMPLETE, BigInt] = new BatchProcessor(P.context[SRC, INCOMPLETE]()).map(_.index)

  def guard[A](reason: INCOMPLETE, value: A = ())(pass: Boolean): BatchProcessor[SRC, INCOMPLETE, A]
    = if (pass) pure(value) else reject(reason)

}

private[batch] object P {

  type Line[SRC, INCOMPLETE, A] = Ctx[SRC] => Either[INCOMPLETE, A]
  type Batch[SRC, INCOMPLETE, A] = Iterable[Ctx[SRC]] => Iterable[Either[INCOMPLETE, A]]

  type Compute[SRC, INCOMPLETE, A] = (Line[SRC, INCOMPLETE, A], Batch[SRC, INCOMPLETE, A])


  def fixed[SRC, INCOMPLETE, A](maybeV: Either[INCOMPLETE, A]): Compute[SRC, INCOMPLETE, A] = {
    (_, _) => maybeV
  }

  def context[SRC, INCOMPLETE](): Compute[SRC, INCOMPLETE, Ctx[SRC]] = {
    (_, ctx) => Right(ctx)
  }

  def join[SRC, INCOMPLETE, A](nested: Compute[SRC, INCOMPLETE, Compute[SRC, INCOMPLETE, A]])
    : Compute[SRC, INCOMPLETE, A] =
  {
    (run: Run[SRC, INCOMPLETE, A], ctx: Ctx[SRC]) =>

  }
//    (null, ctx => nested._2(ctx).right.flatMap(p => p._2(ctx)))


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
      new BatchProcessor(P.join((P.map(this.compute)(f.andThen(_.compute)))))

  private def execute(contexts: Iterable[Context]): Iterable[Result] = P.execute(this.compute)(contexts)

  protected def exec_(batch: Iterable[SRC]): Iterable[Item[SRC, Result]] = {
    val contexts = batch.zipWithIndex.map(p => Ctx(p._1, p._2))
    val results = execute(contexts)
    results.zip(contexts).map(p => Item(p._2.index, p._2.source, p._1))
  }

//  def aggregate[B](summarize: P.Summarize[INCOMPLETE, A, B], f: A => Proc[A]): BatchProcessor

  def run(batch: Iterable[SRC]): Iterable[Item[SRC,A]] = ProcessResult(exec_(batch)).complete
  def exec(batch: Iterable[SRC]): ProcessResult[SRC, INCOMPLETE, A] = ProcessResult(exec_(batch))

}
