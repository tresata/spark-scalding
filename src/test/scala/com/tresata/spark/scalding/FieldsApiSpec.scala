package com.tresata.spark.scalding

import java.lang.{ Integer => JInt, Double => JDouble }
import cascading.tuple.{ Fields, Tuple => CTuple, TupleException }
import com.twitter.scalding.Dsl._
import org.scalatest.FunSpec

class FieldsApiSpec extends FunSpec {
  lazy val sc = SparkSuite.sc

  val rdd1 = new FieldsApi(('a, 'b, 'c), sc.parallelize(List(new CTuple("1", "2", "3"), new CTuple("4", "5", "6"))))
  val rdd2 = new FieldsApi(('a, 'd), sc.parallelize(List(new CTuple("1", "10"), new CTuple("1", "11"))))
  val rdd3 = new FieldsApi(('d, 'e), sc.parallelize(List(new CTuple("1", "10"), new CTuple("1", "11"))))

  describe("A FieldsApi") {
    it("should rename") {
      assert(rdd1.rename('a -> 'd).fields == (('d, 'b, 'c): Fields))
    }

    it("should not allow duplicate fields in rename") {
      intercept[IllegalArgumentException]{ rdd1.rename('a -> 'c) }
    }

    it("should not allow all fields in rename") {
      intercept[TupleException]{ rdd1.rename('* -> 'c) }
      intercept[TupleException]{ rdd1.rename('c -> '*) }
      intercept[TupleException]{ rdd1.rename('* -> '*) }
    }

    it("should discard") {
      val tmp = rdd1.discard('a, 'c)
      assert(tmp.fields == ('b: Fields))
      assert(tmp.rdd.collect.toList == List(new CTuple("2"), new CTuple("5")))
    }

    it("should map replace") {
      val tmp = rdd1.map('a -> 'a){ (x: String) => "7" }
      assert(tmp.fields == (('a, 'b, 'c): Fields))
      assert(tmp.rdd.collect.toList == List(new CTuple("7", "2", "3"), new CTuple("7", "5", "6")))
    }

    it("should map replace to args") {
      val tmp = rdd1.map('a -> Fields.ARGS){ (x: String) => "7" }
      assert(tmp.fields == (('a, 'b, 'c): Fields))
      assert(tmp.rdd.collect.toList == List(new CTuple("7", "2", "3"), new CTuple("7", "5", "6")))
    }

    it("should map append") {
      val tmp = rdd1.map('a -> 'd){ (x: String) => "7" }
      assert(tmp.fields == (('a, 'b, 'c, 'd): Fields))
      assert(tmp.rdd.collect.toList == List(new CTuple("1", "2", "3", "7"), new CTuple("4", "5", "6", "7")))
    }

    it("should map swap") {
      val tmp = rdd1.map('a -> ('a, 'd)){ (x: String) => ("7", "8") }
      assert(tmp.fields == (('b, 'c, 'a, 'd): Fields))
      assert(tmp.rdd.collect.toList == List(new CTuple("2", "3", "7", "8"), new CTuple("5", "6", "7", "8")))
    }

    it("should map all") {
      val tmp = rdd1.map('* -> 'x){ (ctuple: CTuple) => "8" }
      assert(tmp.fields == (('a, 'b, 'c, 'x): Fields))
      assert(tmp.rdd.collect.toList == List(new CTuple("1", "2", "3", "8"), new CTuple("4", "5", "6", "8")))
    }

    it("should map all to all") {
      val tmp = rdd1.map('* -> '*){ (ctuple: CTuple) => new CTuple("0", "0", "0") }
      assert(tmp.fields == (('a, 'b, 'c): Fields))
      assert(tmp.rdd.collect.toList == List(new CTuple("0", "0", "0"), new CTuple("0", "0", "0")))
    }

    it("should map all to args") {
      val tmp = rdd1.map('* -> Fields.ARGS){ (ctuple: CTuple) => new CTuple("0", "0", "0") }
      assert(tmp.fields == (('a, 'b, 'c): Fields))
      assert(tmp.rdd.collect.toList == List(new CTuple("0", "0", "0"), new CTuple("0", "0", "0")))
    }

    it("should mapTo") {
      val tmp = rdd1.mapTo('a -> ('a, 'd)){ (x: String) => ("7", "8") }
      assert(tmp.fields == (('a, 'd): Fields))
      assert(tmp.rdd.collect.toList == List(new CTuple("7", "8"), new CTuple("7", "8")))
    }

    it("should mapTo all to something") {
      val tmp = rdd1.mapTo('* -> ('a, 'd)){ (ctuple: CTuple) => ("7", "8") }
      assert(tmp.fields == (('a, 'd): Fields))
      assert(tmp.rdd.collect.toList == List(new CTuple("7", "8"), new CTuple("7", "8")))
    }

    it("should flatMap replace") {
      val tmp = rdd1.flatMap('a -> 'a){ (x: String) => List("7", "8") }
      assert(tmp.fields == (('a, 'b, 'c): Fields))
      assert(tmp.rdd.collect.toList == List(
        new CTuple("7", "2", "3"), new CTuple("8", "2", "3"),
        new CTuple("7", "5", "6"), new CTuple("8", "5", "6")
      ))
    }

    it("should flatMap replace to args") {
      val tmp = rdd1.flatMap('a -> Fields.ARGS){ (x: String) => List("7", "8") }
      assert(tmp.fields == (('a, 'b, 'c): Fields))
      assert(tmp.rdd.collect.toList == List(
        new CTuple("7", "2", "3"), new CTuple("8", "2", "3"),
        new CTuple("7", "5", "6"), new CTuple("8", "5", "6")
      ))
    }

    it("should flatMap append") {
      val tmp = rdd1.flatMap('a -> 'd){ (x: String) => List("7", "8") }
      assert(tmp.fields == (('a, 'b, 'c, 'd): Fields))
      assert(tmp.rdd.collect.toList == List(
        new CTuple("1", "2", "3", "7"), new CTuple("1", "2", "3", "8"),
        new CTuple("4", "5", "6", "7"), new CTuple("4", "5", "6", "8"))
      )
    }

    it("should flatMap swap") {
      val tmp = rdd1.flatMap('a -> ('a, 'd)){ (x: String) => List(("7", "8"), ("9", "10")) }
      assert(tmp.fields == (('b, 'c, 'a, 'd): Fields))
      assert(tmp.rdd.collect.toList == List(
        new CTuple("2", "3", "7", "8"), new CTuple("2", "3", "9", "10"),
        new CTuple("5", "6", "7", "8"), new CTuple("5", "6", "9", "10")
      ))
    }

    it("should flatMap all") {
      val tmp = rdd1.flatMap('* -> 'x){ (ctuple: CTuple) => List("8", "9") }
      assert(tmp.fields == (('a, 'b, 'c, 'x): Fields))
      assert(tmp.rdd.collect.toList == List(
        new CTuple("1", "2", "3", "8"), new CTuple("1", "2", "3", "9"),
        new CTuple("4", "5", "6", "8"), new CTuple("4", "5", "6", "9")
      ))
    }

    it("should flatMap all to all") {
      val tmp = rdd1.flatMap('* -> '*){ (ctuple: CTuple) => List(new CTuple("0", "0", "0"), new CTuple("1", "1", "1")) }
      assert(tmp.fields == (('a, 'b, 'c): Fields))
      assert(tmp.rdd.collect.toList == List(
        new CTuple("0", "0", "0"), new CTuple("1", "1", "1"),
        new CTuple("0", "0", "0"), new CTuple("1", "1", "1")
      ))
    }

    it("should flatMap all to args") {
      val tmp = rdd1.flatMap('* -> Fields.ARGS){ (ctuple: CTuple) => List(new CTuple("0", "0", "0"), new CTuple("1", "1", "1")) }
      assert(tmp.fields == (('a, 'b, 'c): Fields))
      assert(tmp.rdd.collect.toList == List(
        new CTuple("0", "0", "0"), new CTuple("1", "1", "1"),
        new CTuple("0", "0", "0"), new CTuple("1", "1", "1")
      ))
    }

    it("should flatMapTo") {
      val tmp = rdd1.flatMapTo('a -> ('a, 'd)){ (x: String) => List(("7", "8"), ("9", "10")) }
      assert(tmp.fields == (('a, 'd): Fields))
      assert(tmp.rdd.collect.toList == List(
        new CTuple("7", "8"), new CTuple("9", "10"),
        new CTuple("7", "8"), new CTuple("9", "10")
      ))
    }

    it("should flatMapTo all to something") {
      val tmp = rdd1.flatMapTo('* -> ('a, 'd)){ (ctuple: CTuple) => List(("7", "8"), ("9", "10")) }
      assert(tmp.fields == (('a, 'd): Fields))
      assert(tmp.rdd.collect.toList == List(
        new CTuple("7", "8"), new CTuple("9", "10"),
        new CTuple("7", "8"), new CTuple("9", "10")
      ))
    }

    it("should filter") {
      val tmp = rdd1.filter('a){ a: String => a == "1" }
      assert(tmp.fields == (('a, 'b, 'c): Fields))
      assert(tmp.rdd.collect.toList == List(new CTuple("1", "2", "3")))
    }

    it("should insert") {
      val tmp = rdd1.insert('d, 1: JInt)
      assert(tmp.fields == (('a, 'b, 'c, 'd): Fields))
      assert(tmp.rdd.collect.toList == List(new CTuple("1", "2", "3", 1: JInt), new CTuple("4", "5", "6", 1: JInt)))
    }

    it("should join with same keys") {
      val tmp = rdd1.join('a, rdd2)
      assert(tmp.fields == (('a, 'b, 'c, 'd): Fields))
      assert(tmp.rdd.collect.toList == List(new CTuple("1", "2", "3", "10"), new CTuple("1", "2", "3", "11")))
    }

    it("should join with different keys") {
      val tmp = rdd1.join('a -> 'd, rdd3)
      assert(tmp.fields == (('a, 'b, 'c, 'd, 'e): Fields))
      assert(tmp.rdd.collect.toList == List(new CTuple("1", "2", "3", "1", "10"), new CTuple("1", "2", "3", "1", "11")))
    }

    it("should left join") {
      val tmp = rdd1.leftJoin('a -> 'd, rdd3)
      assert(tmp.fields == (('a, 'b, 'c, 'd, 'e): Fields))
      assert(tmp.rdd.collect.toList == List(new CTuple("1", "2", "3", "1", "10"), new CTuple("1", "2", "3", "1", "11"), new CTuple("4", "5", "6", null, null)))
    }

    it("should groupBy") {
      val tmp = rdd1
        .insert('g, "group1")
        .groupBy('g)(_
          .average('a -> 'avgA)
          .size('size)
          .mapPlusMap('b -> 'setB){ b: String => Set(b) }{ s: Set[String] => s.mkString(",") }
        )
        .map('avgA -> 'avgA)(identity[String])
        .map('size -> 'size)(identity[String])
      assert(tmp.fields == (('g, 'avgA, 'size, 'setB): Fields))
      assert(tmp.rdd.collect.toList == List(new CTuple("group1", "2.5", "2", "2,5")))
    }
  }

  it("should support unpivot and pivot") {
    val tmp1 = rdd1.unpivot(('b, 'c) -> ('key, 'value))
    assert(tmp1.fields == (('a, 'key, 'value): Fields))
    assert(tmp1.rdd.collect.toList == List(
      new CTuple("1", "b", "2"), new CTuple("1", "c", "3"),
      new CTuple("4", "b", "5"), new CTuple("4", "c", "6")
    ))
    val tmp2 = tmp1.groupBy('a)(_.pivot(('key, 'value) -> ('b, 'c)))
    assert(tmp2.fields == (('a, 'b, 'c): Fields))
    assert(tmp2.rdd.collect.toList == List(new CTuple("1", "2", "3"), new CTuple("4", "5", "6")))
  }
}
