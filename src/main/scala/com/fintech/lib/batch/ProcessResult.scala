package com.fintech.lib.batch

case class ProcessResult[SRC, INCOMPLETE, +A](all: Iterable[Item[SRC, Either[INCOMPLETE, A]]]) {
  def incomplete: Iterable[Item[SRC, INCOMPLETE]] = all.filter(_.value.isLeft).map(_.map(_.left.get))
  def complete: Iterable[Item[SRC, A]] = all.filter(_.value.isRight).map(_.map(_.right.get))
}

