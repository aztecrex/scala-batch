package com.cj.fintech.lib.batch

case class Item[SRC, +T](index: BigInt, source: SRC, value: T) {
  def map[U](f: T => U): Item[SRC, U] = Item(index, source, f(value))
}
