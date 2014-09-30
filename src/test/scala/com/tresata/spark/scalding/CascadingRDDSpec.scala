package com.tresata.spark.scalding

import cascading.tuple.Fields
import com.twitter.scalding.Csv
import org.scalatest.FunSpec
import com.twitter.scalding.Dsl._

class CascadingRDDSpec extends FunSpec {
  lazy val sc = SparkSuite.sc
  val source = Csv("file://" + new java.io.File("test/data/fruits.bsv").getAbsolutePath, separator = "|", skipHeader = true, writeHeader = true)
  val fruits = CascadingRDD(sc, source)

  describe("CascadingRDD") {
    it("should have correct fields") {
      assert(fruits.fields == new Fields("name", "color", "quantity"))
    }

    it("should support conversion to typed") {
      assert(fruits.typed[(String, String, Int)]().collect.toList == List(("apple", "red", 5), ("grape", null, 50), ("pear", "green", 3)))
      assert(fruits.typed[(String, Int)]('name, 'quantity).collect.toList == List(("apple", 5), ("grape", 50), ("pear", 3)))
    }

    it("should support saving to source") {
      val source1 = Csv("file://" + new java.io.File("tmp/fruits1.bsv").getAbsolutePath, separator = "|", skipHeader = true, writeHeader = true)
      CascadingRDD.saveToSource(fruits, source1)
      val fruits1 = CascadingRDD(sc, source1)
      assert(fruits.collect.toList == fruits1.collect.toList)
    }

    it("should convert to tuple entries") {
      val source2 = Csv("file://" + new java.io.File("tmp/fruits2.bsv").getAbsolutePath, separator = "|", skipHeader = true, writeHeader = true)
      CascadingRDD.saveToSource(fruits.tupleEntries, source2)
      val fruits2 = CascadingRDD(sc, source2)
      assert(fruits.collect.toList == fruits2.collect.toList)
    }
  }

  describe("FieldsRDD") {
    val fieldsRDD = FieldsRDD(fruits.fields)(fruits.map(identity))

    it("should have correct fields") {
      assert(fieldsRDD.fields == fruits.fields)
    }

    it("should support conversion to and from typed") {
      val typed = fruits.typed[(String, Int)]('name, 'quantity)
      assert(FieldsRDD('name, 'quantity)(typed).typed[(String, Int)]().collect.toList == typed.collect.toList)
    }

    it("should save correcly") {
      val source3 = Csv("file://" + new java.io.File("tmp/fruits3.bsv").getAbsolutePath, separator = "|", skipHeader = true, writeHeader = true)
      CascadingRDD.saveToSource(fieldsRDD, source3)
      val fruits3 = CascadingRDD(sc, source3)
      assert(fruits.collect.toList == fruits3.collect.toList)
    }
  }
}
