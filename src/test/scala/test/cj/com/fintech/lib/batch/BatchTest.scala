package test.cj.com.fintech.lib.batch

import com.cj.fintech.lib.batch.{BatchContext, Item}
import org.scalatest.FunSuite
import Function.const

class BatchTest extends FunSuite {


  test("batch program runs") {

    // given
    val context = BatchContext[String, String]
    import context._
    val batch = Seq("a", "b")
    val f = {s: String => s + " awesome"}
    val processor = source.map(f)

    // when
    val actual = processor.run(batch)

    // then
    assert(actual.map(_.value) === batch.map(f))

  }

  test("batch program tracks line number") {

    // given
    val context = BatchContext[String, String]
    import context._
    val batch = Seq("a", "b")
    val f = {s: String => s + " awesome"}
    val processor = source().map(f)

    // when
    val actual = processor.run(batch)

    // then
    assert(actual.map(_.index) === batch.zipWithIndex.map(_._2))

  }

  test("batch program expresses line failure") {

    // given
    val context = BatchContext[String, String]
    import context._
    val ok = "a"
    val batch = Seq("x",ok,"z")
    val test = {s: String => s == ok}
    val guard = { s: String => if (test(s)) pure(s) else reject("not ok")}
    val p = source()
    val processor = p.flatMap(guard)

    // when
    val actual = processor.run(batch)

    // then
    assert(actual.map(_.value) === batch.filter(test))

  }

  test("batch can handle non-string types") {

    // given
    val context = BatchContext[Int, String]
    import context._
    val batch = Seq(100, 200)
    val processor = source()

    // when
    val actual = processor.run(batch)

    // then
    assert(actual.map(_.value) === batch)

  }

  test("batch retains original") {

    // given
    val context = BatchContext[Int, String]
    import context._
    val batch = Seq(101, 202)
    val processor = source().map(_ + 1)

    // when
    val actual = processor.run(batch)

    // then
    assert(actual.map(_.source) === batch)

  }

  test("batch mappable context when continue") {

    // given
    val context = BatchContext[Int, String]
    import context._
    val batch = Seq(202, 303)
    val processor = source()
    val f = {i: Int => i + 3}

    // when
    val actual = processor.map(f).run(batch)


    // then
    assert(actual.map(_.value) === batch.map(f))

  }

  test("batch mappable context when aborted") {

    // given
    val context = BatchContext[Int, String]
    import context._
    val batch = Seq(202, 303)
    val reason = "bleh"
    val processor = reject(reason)
    val f = {i: Int => i + 3}

    // when
    val actual = processor.map(f).exec(batch)


    // then
    assert(actual.complete.isEmpty)
    assert(actual.incomplete.map(_.value) === batch.map(const(reason)))


  }

  test("batch flat-mappable context when continue") {

    // given
    val context = BatchContext[Int, String]
    import context._
    val batch = Seq(51, 61)
    val change = {i: Int => i + 1}
    val processor = source().map(change)
    val f = {a: Int => source().map(src => (src, a))}

    // when
    val actual = processor.flatMap(f).run(batch)

    // then
    assert(actual.map(_.value) === batch.map(i => (i, change(i))))
  }

  test("batch flat-mappable context when aborted") {

    // given
    val context = BatchContext[Int, String]
    import context._
    val batch = Seq(55, 66)
    val reason = "blah"
    val processor = reject(reason)
    val f = {i: Int => pure(i)}

    // when
    val actual = processor.flatMap(f).exec(batch)

    // then
    assert(actual.complete.isEmpty)
    assert(actual.incomplete.map(_.value) === batch.map(const(reason)))

  }

  test("capture abort values") {

    // given
    val context = BatchContext[Int, Symbol]
    import context._
    val batch = Seq(202, 303)
    val reason = 'NoGood

    val processor = reject(reason)

    // when
    val actual = processor.exec(batch)

    // then
    assert(actual.incomplete.map(_.value) === batch.map(const(reason)))

  }

  test("complete and incomplete in same batch") {

    // given
    val context = BatchContext[String, Symbol]
    import context._
    val ok = "ok"
    val reason = 'NotOK
    val batch = Seq("x", ok, "y")
    val test = {s: String => s == ok}
    val processor = source().flatMap(s => if (test(s)) pure(s) else reject(reason) )

    // when
    val actual = processor.exec(batch)

    // then
    val expected = batch.zipWithIndex.map(p => (p._2, p._1, if (test(p._1)) Right(p._1) else Left(reason) ))
    assert(actual.complete === expected.filter(t => t._3.isRight).map(t => Item(t._1, t._2, t._3.right.get)))
    assert(actual.incomplete === expected.filter(t => t._3.isLeft).map(t => Item(t._1, t._2,t._3.left.get)))

  }

  test("reject") {
    // given
    val context = BatchContext[Int, String]
    import context._
    val batch = Seq(1,2)
    val reason = "what"
    val processor = reject(reason)

    // when
    val actual = processor.exec(batch)

    // then
    assert(actual.complete.isEmpty)
    assert(actual.incomplete.map(_.value) === batch.map(const(reason)))
  }


  test("pure") {

    // given
    val context = BatchContext[Int, String]()
    import context._
    val batch = Seq(1,2)
    val v = "what"
    val processor = pure(v)

    // when
    val actual = processor.exec(batch)

    // then
    assert(actual.complete.map(_.value) === batch.map(const(v)))
    assert(actual.incomplete.isEmpty)
  }

  test("source") {

    // given
    val context = BatchContext[Int, Int]()
    import context._
    val batch = Seq(1,2)
    val processor = source()

    // when
    val actual = processor.exec(batch)

    // then
    assert(actual.complete.map(_.value) === batch)
    assert(actual.incomplete.isEmpty)

  }

  test("demo1") {

    // given
    val context = BatchContext[Int, Symbol]
    import context._
    val batch = Seq(5, 4, 3, 2, 1)
    val bigEnough = {x: Int => x >= 3}
    val bigEnough_ = {x: Int => guard('Small)(bigEnough(x))}
    val processor = for {
      i <- source()
      j = i + 1
      k <- pure(j * 17)
      _ <- bigEnough_(i)
      v  = k.toString()
    } yield v

    // when
    val actual = processor.run(batch)

    // then
    val equivalent = {i: Int => ((i + 1) * 17).toString()}
    assert(actual === batch.zipWithIndex.filter(p => bigEnough(p._1)).map(p => Item(p._2, p._1, equivalent(p._1))))

  }

  test("index") {

    // given
    val context = BatchContext[Int, Symbol]
    import context._
    val batch = Seq(2, 3, 4, 5, 300)
    val processor = for {
      i <- index()
      src <- source()
      v = i * src
    } yield v

    // when
    val actual = processor.run(batch)

    // then
    assert(actual.map(_.value) === batch.zipWithIndex.map(p => p._1 * p._2))

  }

  test("demo 2") {

    // given
    val context = BatchContext[String, Symbol]
    import context._
    val batch = Seq("header 1 - corn", "header 2 - cow", "data 1", "data 2")
    val numHeaders = 2
    val processor = for {
      idx <- index()
      _ <- guard('Header)(idx >= numHeaders)
      src <- source()
      ans = src.reverse
    } yield ans

    // when
    val actual = processor.run(batch)

    // then
    assert(actual.map(_.value) === batch.drop(numHeaders).map(_.reverse))

  }

  test("guard") {

    // given
    val context = BatchContext[Int, Symbol]
    import context._
    val batch = Seq(-3, 1, 2, 0, -12, -100, 24)
    val test = {v: Int => v < 0}
    val bad = 'Bad
    val processor = source().map(test(_)).flatMap((test: Boolean) => guard(bad)(test))

    // when
    val actual = processor.exec(batch)


    // then
    val expected =
      batch
          .map(v => if (test(v)) Right(()) else Left(bad))
          .zip(batch)
          .zipWithIndex
          .map(p => Item(p._2, p._1._2, p._1._1 ))
    assert(actual.all === expected)

  }

  test("fold left") {
    // given
    val context = BatchContext[String, Symbol]
    import context._

    val batch = Seq("a", "b", "c")
    val init: BigInt = 7
    val f = {(x: BigInt, y: BigInt) => x + y}
    val processor = index().map(_ + 1).foldLeft(init)(f)

    // when
    val actual = processor.exec(batch)

    val sum = batch.zipWithIndex.map(_._2 + 1).map(BigInt(_)).foldLeft(init)(f)
    assert(actual.complete === batch.zipWithIndex.map(p => Item(p._2, p._1, sum)))
    assert(actual.incomplete.isEmpty)



  }

  test("fold does not consider rejected") {

    // given
    val context = BatchContext[Int, Symbol]
    val bad = 150
    val batch = Seq(100, bad, 200)

    val sum = {(x: Int, ag: Int) => x + ag}

    val processor = context
      .source()
      .flatMap({v: Int => if (v == bad) context.reject('Bad) else context.pure(v)})
      .foldLeft(0)(sum)

    // when
    val actual = processor.run(batch)

    // then
    val considered = batch.filter(_ != bad)
    val summary = considered.foldLeft(0)(sum)
    val expected = considered.map(const(summary))
    assert(actual.map(_.value) === expected)

  }

  test("fold propagates rejections") {

    // given
    val context = BatchContext[Int, Symbol]
    val bad = 150
    val batch = Seq(100, bad, 200)

    val sum = {(x: Int, ag: Int) => x + ag}

    val processor = context
      .source()
      .flatMap({v: Int => if (v == bad) context.reject('Bad) else context.pure(v)})
      .foldLeft(0)(sum)

    // when
    val actual = processor.exec(batch)

    // then
    assert(actual.incomplete == batch.zipWithIndex.filter(_._1 == bad).map(p => Item(p._2, p._1, 'Bad)))

  }

  test("demo 3") {

    // given
    val context = BatchContext[Int, Symbol]
    val batch = Seq(1, 2, 3, 5, 4, 300)

    val processor = for {
      src <- context.source()
      x = BigDecimal(src)
      sum <- context.pure(x).foldLeft(BigDecimal(0))((a, agg) => a + agg)
      ans <- context.pure(x / sum)
    } yield sum

    // when
    val actual = processor.run(batch)
    println(actual)

    fail()

  }


}
