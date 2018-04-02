package com.fintech.lib.batch

case class BatchContext[SRC, INCOMPLETE]() {
  def reject[A](reason: INCOMPLETE): Processor[SRC, INCOMPLETE, A]
  = Processor(Function.const(Left(reason)))

  def pure[A](value: A): Processor[SRC, INCOMPLETE, A]
  = Processor(Function.const(Right(value)))

  def source(): Processor[SRC, INCOMPLETE, SRC]
  = Processor(Right(_))
}


case class Item[SRC, +T](index: BigInt, source: SRC, value: T) {
  def map[U](f: T => U): Item[SRC, U] = Item(index, source, f(value))
}

case class ProcessResult[SRC, INCOMPLETE, +A](all: Iterable[Item[SRC, Either[INCOMPLETE, A]]]) {
  def incomplete: Iterable[Item[SRC, INCOMPLETE]] = all.filter(_.value.isLeft).map(_.map(_.left.get))
  def complete: Iterable[Item[SRC, A]] = all.filter(_.value.isRight).map(_.map(_.right.get))
}

case class Processor[SRC, INCOMPLETE, +A](runLine: SRC => Either[INCOMPLETE, A]) {

  def flatMap[B](f: A => Processor[SRC, INCOMPLETE, B]): Processor[SRC, INCOMPLETE, B]
  = Processor(src => runLine(src).right.map(f).right.flatMap(_.runLine(src)))

  def map[B](f: A => B): Processor[SRC, INCOMPLETE, B] = {
    Processor(src => runLine(src).right.map(f))
  }

  private def exec_(batch: Iterable[SRC]): ProcessResult[SRC, INCOMPLETE, A]
  = ProcessResult(batch.zipWithIndex.map(p => Item(p._2, p._1, runLine(p._1))))

  def run(batch: Iterable[SRC]): Iterable[Item[SRC,A]] = {
    exec_(batch).complete
  }

  def exec(batch: Iterable[SRC]): ProcessResult[SRC, INCOMPLETE, A] = exec_(batch)

}
