package com.fintech.lib.batch

case class Ctx[SRC](source: SRC, index: BigInt)

case class BatchContext[SRC, INCOMPLETE]() {

  def reject[A](reason: INCOMPLETE): AltProcessor[SRC, INCOMPLETE, A]
    = new AltProcessor(Function.const(Left(reason)))

  def pure[A](value: A): AltProcessor[SRC, INCOMPLETE, A]
      = new AltProcessor(Function.const(Right(value)))

  def source(): AltProcessor[SRC, INCOMPLETE, SRC]
    = new AltProcessor(ctx => Right(ctx.source))

  def index(): AltProcessor[SRC, INCOMPLETE, BigInt]
    = new AltProcessor(ctx => Right(ctx.index))
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


class AltProcessor[SRC, INCOMPLETE, +A](private val runLine: Ctx[SRC] => Either[INCOMPLETE, A]) {

  def fold[B](initial: B)(f: (B, A) => B): AltProcessor[SRC, INCOMPLETE, B] = {

    val x = {ctx: Ctx[SRC] =>
      val summary = initial
      val maybeA = runLine(ctx)
      maybeA.right.map(Function.const(summary))
    }
    new AltProcessor(x)

//    new BatchProcessor({sources: Iterable[SRC] =>
//      val unpacked = r(sources)
//      val summary = unpacked.filter(_.isRight).map(_.right.get).foldLeft[B](initial)(f)
//      unpacked.map(x => x.right.map(Function.const(summary)))
//    })
    ???
  }

  def map[B](f: A => B): AltProcessor[SRC, INCOMPLETE, B]
    = new AltProcessor({ctx: Ctx[SRC] => runLine(ctx).right.map(f)})

  def flatMap[B](f: A => AltProcessor[SRC, INCOMPLETE, _ <: B]): AltProcessor[SRC, INCOMPLETE, B]
    = new AltProcessor({ctx: Ctx[SRC] => runLine(ctx).right.map(f).right.map(_.runLine(ctx)).joinRight})

  protected def exec_(batch: Iterable[SRC]): Iterable[Either[INCOMPLETE,A]] = {
    batch.zipWithIndex.map(pp => Ctx(pp._1, pp._2)).map(runLine)
  }

  private def result(batch: Iterable[SRC]): ProcessResult[SRC, INCOMPLETE, A]
    = ProcessResult(exec_(batch).zip(batch).zipWithIndex.map(pp => Item(pp._2, pp._1._2, pp._1._1)))

  def run(batch: Iterable[SRC]): Iterable[Item[SRC,A]] = result(batch).complete
  def exec(batch: Iterable[SRC]): ProcessResult[SRC, INCOMPLETE, A] = result(batch)
}

