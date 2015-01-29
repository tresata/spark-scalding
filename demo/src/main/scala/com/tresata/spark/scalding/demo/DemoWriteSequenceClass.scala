package com.tresata.spark.scalding.demo

import com.twitter.scalding.{ SequenceFile, Csv, Job, Args }
import com.tresata.spark.scalding.SparkJob

/**
 * Created by inikolov on 29/01/15.
 */
class DemoWriteSequenceClass(args: Args) extends SparkJob(args) {
  override def run: Boolean = {
    Csv(args("input"), separator = "|", skipHeader = true).spark.fieldsApi
      .mapTo(('name, 'color, 'quantity) -> 'fruitLine) { x: (String, String, String) =>
        val (name, color, quantity) = x
        FruitLine(name, color, quantity.toInt)
      }
      .write(SequenceFile(args("output")))

    true
  }
}
