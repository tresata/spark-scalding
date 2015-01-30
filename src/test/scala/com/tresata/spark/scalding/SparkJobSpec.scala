package com.tresata.spark.scalding

import java.io.File
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import cascading.tuple.{ Fields, Tuple => CTuple }
import com.twitter.scalding.{ Args, Csv, Mode, SequenceFile }
import com.twitter.scalding.FunctionImplicits._
import org.scalatest.FunSpec

case class Fruit(name: String, color: String, quantity: Int)

class TestJobWriteReadCsv(args: Args) extends SparkJob(args) {
  // we want to use the local spark context for unit tests
  override implicit lazy val sc: SparkContext = SparkSuite.sc
  
  override def run: Boolean = {
    val rdd1 = FieldsRDD((('name, 'color, 'quantity): Fields))(sc.parallelize(List(
      new CTuple("apple", "red", "5"),
      new CTuple("grape", null, "50"),
      new CTuple("pear", "green", "3"))))
    rdd1.fieldsApi.write(Csv(args("tmp"), separator = "|", writeHeader = true))

    val rdd2 = Csv(args("tmp"), separator = "|", skipHeader = true).spark

    rdd1.fields == rdd2.fields && rdd1.collect.toList == rdd2.collect.toList
  }
}

class TestJobWriteReadSeqKryo(args: Args) extends SparkJob(args) {
  // we want to use the local spark context for unit tests
  override implicit lazy val sc: SparkContext = SparkSuite.sc
  
  override def run: Boolean = {
    val rdd1 = FieldsRDD((('name, 'color, 'quantity): Fields))(sc.parallelize(List(
      new CTuple("apple", "red", "5"),
      new CTuple("grape", null, "50"),
      new CTuple("pear", "green", "3"))))
      .fieldsApi
      .mapTo(('name, 'color, 'quantity) -> 'fruit) { (name: String, color: String, quantity: Int) => Fruit(name, color, quantity) }
      .fieldsRDD

    rdd1.fieldsApi.write(SequenceFile(args("tmp")))

    val rdd2 = SequenceFile(args("tmp"), 'fruit).spark

    rdd1.fields == rdd2.fields && rdd1.collect.toList == rdd2.collect.toList
  }
}

class SparkJobSpec extends FunSpec {
  new File("tmp").mkdir()

  describe("A SparkJob") {
    it("should roundtrip csv") {
      com.twitter.scalding.Tool.main("com.tresata.spark.scalding.TestJobWriteReadCsv --hdfs --tmp tmp/roundtrip_csv".split("\\s+"))
    }

    it("should roundtrip sequencefile with kryo serialized case class") {
      com.twitter.scalding.Tool.main("com.tresata.spark.scalding.TestJobWriteReadSeqKryo --hdfs --tmp tmp/roundtrip_seq_kryo".split("\\s+"))
    }
  }
}
