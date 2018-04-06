package com.fintech.lib.batch
import Function.const
case class Ctx[SRC](source: SRC, index: BigInt)

case class BatchContext[SRC, INCOMPLETE]() {

  type Processor[A] = BatchProcessor[SRC, INCOMPLETE, A]

  def reject[A](reason: INCOMPLETE): BatchProcessor[SRC, INCOMPLETE, A]
    = new BatchProcessor(const(Left(reason)))

  def pure[A](value: A): BatchProcessor[SRC, INCOMPLETE, A]
      = new BatchProcessor(const(Right(value)))

  def source(): BatchProcessor[SRC, INCOMPLETE, SRC]
    = new BatchProcessor(ctx => Right(ctx.source))

  def index(): BatchProcessor[SRC, INCOMPLETE, BigInt]
    = new BatchProcessor(ctx => Right(ctx.index))

  def guard(reason: INCOMPLETE)(test: Boolean): BatchProcessor[SRC, INCOMPLETE, Unit]
    = if (test) pure(()) else reject(reason)

}

class BatchProcessor[SRC, INCOMPLETE, +A](private val runLine: Ctx[SRC] => Either[INCOMPLETE, A]) {

  def fold[B](initial: B)(f: (B, A) => B): BatchProcessor[SRC, INCOMPLETE, B] = {

    val x = {ctx: Ctx[SRC] =>
      val summary = initial
      val maybeA = runLine(ctx)
      maybeA.right.map(Function.const(summary))
    }
    new BatchProcessor(x)

//    new BatchProcessor({sources: Iterable[SRC] =>
//      val unpacked = r(sources)
//      val summary = unpacked.filter(_.isRight).map(_.right.get).foldLeft[B](initial)(f)
//      unpacked.map(x => x.right.map(Function.const(summary)))
//    })
    ???
  }

  def map[B](f: A => B): BatchProcessor[SRC, INCOMPLETE, B]
    = new BatchProcessor({ ctx: Ctx[SRC] => runLine(ctx).right.map(f)})

  def flatMap[B](f: A => BatchProcessor[SRC, INCOMPLETE, _ <: B]): BatchProcessor[SRC, INCOMPLETE, B]
    = new BatchProcessor({ ctx: Ctx[SRC] => runLine(ctx).right.map(f).right.map(_.runLine(ctx)).joinRight})

  protected def exec_(batch: Iterable[SRC]): Iterable[Either[INCOMPLETE,A]] = {
    batch.zipWithIndex.map(pp => Ctx(pp._1, pp._2)).map(runLine)
  }

  private def result(batch: Iterable[SRC]): ProcessResult[SRC, INCOMPLETE, A]
    = ProcessResult(exec_(batch).zip(batch).zipWithIndex.map(pp => Item(pp._2, pp._1._2, pp._1._1)))

  def run(batch: Iterable[SRC]): Iterable[Item[SRC,A]] = result(batch).complete
  def exec(batch: Iterable[SRC]): ProcessResult[SRC, INCOMPLETE, A] = result(batch)
}

