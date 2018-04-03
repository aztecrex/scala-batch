package test.cj.com.fintech.lib.batch

import com.fintech.lib.batch.{BatchContext, Item, Processor}
import org.scalatest.FunSuite

class BatchTest extends FunSuite {


  test("batch program runs") {


    // given
    val context = BatchContext[String, String]
    val batch = Seq("a", "b")
    val f = {s: String => s + " awesome"}
    val processor = context.source.map(f)

    // when
    val actual = processor.run(batch)

    // then
    assert(actual.map(_.value) === batch.map(f))

  }

  test("batch program tracks line number") {

    // given
    val context = BatchContext[String, String]
    val batch = Seq("a", "b")
    val f = {s: String => s + " awesome"}
    val processor = context.source().map(f)

    // when
    val actual = processor.run(batch)

    // then
    assert(actual.map(_.index) === batch.zipWithIndex.map(_._2))

  }

  test("batch program expresses line failure") {

    // given
    val context = BatchContext[String, String]
    val ok = "a"
    val batch = Seq("x",ok,"z")
    val test = {s: String => s == ok}
    val guard = { s: String => if (test(s)) context.pure(s) else context.reject("not ok")}
    val p = context.source()
    val processor = p.flatMap(guard)

    // when
    val actual = processor.run(batch)

    // then
    assert(actual.map(_.value) === batch.filter(test))

  }

  test("batch can handle non-string types") {

    // given
    val context = BatchContext[Int, String]
    val batch = Seq(100, 200)
    val processor = context.source()

    // when
    val actual = processor.run(batch)

    // then
    assert(actual.map(_.value) === batch)

  }

  test("batch retains original") {

    // given
    val context = BatchContext[Int, String]
    val batch = Seq(101, 202)
    val processor = context.source().map(_ + 1)

    // when
    val actual = processor.run(batch)

    // then
    assert(actual.map(_.source) === batch)

  }

  test("batch mappable context when continue") {

    // given
    val context = BatchContext[Int, String]
    val batch = Seq(202, 303)
    val processor = context.source()
    val f = {i: Int => i + 3}

    // when
    val actual = processor.map(f).run(batch)


    // then
    assert(actual.map(_.value) === batch.map(f))

  }

  test("batch mappable context when aborted") {

    // given
    val context = BatchContext[Int, String]
    val batch = Seq(202, 303)
    val reason = "bleh"
    val processor = context.reject(reason)
    val f = {i: Int => i + 3}

    // when
    val actual = processor.map(f).exec(batch)


    // then
    assert(actual.complete.isEmpty)
    assert(actual.incomplete.map(_.value) === batch.map(Function.const(reason)))


  }

  test("batch flat-mappable context when continue") {

    // given
    val context = BatchContext[Int, String]
    val batch = Seq(51, 61)
    val change = {i: Int => i + 1}
    val processor = context.source().map(change)
    val f = {a: Int => context.source().map(src => (src, a))}

    // when
    val actual = processor.flatMap(f).run(batch)

    // then
    assert(actual.map(_.value) === batch.map(i => (i, change(i))))
  }

  test("batch flat-mappable context when aborted") {

    // given
    val context = BatchContext[Int, String]
    val batch = Seq(55, 66)
    val reason = "blah"
    val processor = context.reject(reason)
    val f = {i: Int => context.pure(i)}

    // when
    val actual = processor.flatMap(f).exec(batch)

    // then
    assert(actual.complete.isEmpty)
    assert(actual.incomplete.map(_.value) === batch.map(Function.const(reason)))

  }

  test("capture abort values") {

    // given
    val context = BatchContext[Int, Symbol]
    val batch = Seq(202, 303)
    val reason = 'NoGood

    val processor = context.reject(reason)

    // when
    val actual = processor.exec(batch)

    // then
    assert(actual.incomplete.map(_.value) === batch.map(Function.const(reason)))

  }

  test("complete and incomplete in same batch") {

    // given
    val context = BatchContext[String, Symbol]
    val ok = "ok"
    val reason = 'NotOK
    val batch = Seq("x", ok, "y")
    val test = {s: String => s == ok}
    val processor = context.source().flatMap(s => if (test(s)) context.pure(s) else context.reject(reason) )

    // when
    val actual = processor.exec(batch)

    // then
    val expected = batch.zipWithIndex.map(p => (p._2, p._1, if (test(p._1)) Right(p._1) else Left(reason) ))
    assert(actual.complete === expected.filter(t => t._3.isRight).map(t => Item(t._1, t._2, t._3.right.get)))
    assert(actual.incomplete === expected.filter(t => t._3.isLeft).map(t => Item(t._1, t._2,t._3.left.get)))

  }

  test("abort") {
    // given
    val context = BatchContext[Int, String]
    val batch = Seq(1,2)
    val reason = "what"
    val processor = context.reject(reason)

    // when
    val actual = processor.exec(batch)

    // then
    assert(actual.complete.isEmpty)
    assert(actual.incomplete.map(_.value) === batch.map(Function.const(reason)))
  }


  test("pure") {

    // given
    val context = BatchContext[Int, String]()
    val batch = Seq(1,2)
    val v = "what"
    val processor = context.pure(v)

    // when
    val actual = processor.exec(batch)

    // then
    assert(actual.complete.map(_.value) === batch.map(Function.const(v)))
    assert(actual.incomplete.isEmpty)
  }

  test("source") {

    // given
    val context = BatchContext[Int, Int]()
    val batch = Seq(1,2)
    val processor = context.source()

    // when
    val actual = processor.exec(batch)

    // then
    assert(actual.complete.map(_.value) === batch)
    assert(actual.incomplete.isEmpty)

  }

  test("demo1") {

    // given
    val context = BatchContext[Int, Symbol]
    val batch = Seq(5, 4, 3, 2, 1)
    val bad = {x: Int => x < 3}
    val processor = for {
      i <- context.source()
      j = i + 1
      k <- context.pure(j * 17)
      _ <- if (bad(i)) context.reject('Small) else context.pure(())
      v  = k.toString()
    } yield v

    // when
    val actual = processor.run(batch)

    // then
    val equivalent = {i: Int => ((i + 1) * 17).toString()}
    assert(actual === batch.zipWithIndex.filter(p => !bad(p._1)).map(p => Item(p._2, p._1, equivalent(p._1))))

  }


}
