package com.tresata.spark.scalding.demo

import com.tresata.spark.scalding.SparkJob
import com.twitter.scalding.{Args, Csv, SequenceFile}

/**
 * Created by Ivan Nikolov <nikolovivann@gmail.com> on 30/01/15.
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
