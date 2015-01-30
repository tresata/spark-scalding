package com.tresata.spark.scalding.demo

import com.twitter.scalding.{ Args, Csv, SequenceFile }
import com.tresata.spark.scalding.SparkJob

class DemoSequenceFileJob(args: Args) extends SparkJob(args) {
  override def run: Boolean = {
    SequenceFile(args("input"), ('name, 'color, 'quantity)).spark.fieldsApi
      .discard('color)
      .groupBy('name){ _
        .sum[Int]('quantity)
        .size
      }
      .write(Csv(args("output"), separator = "|", writeHeader = true))
    true
  }
}