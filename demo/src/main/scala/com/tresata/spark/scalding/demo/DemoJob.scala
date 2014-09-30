package com.tresata.spark.scalding.demo

import com.twitter.scalding.{ Args, Csv }
import com.tresata.spark.scalding.SparkJob

class DemoJob(args: Args) extends SparkJob(args) {
  override def run: Boolean = {
    Csv(args("input"), separator = "|", skipHeader = true).spark.fieldsApi
      .discard('color)
      .groupBy('name){ _
        .sum[Int]('quantity)
        .size
      }
      .write(Csv(args("output"), separator = "|", writeHeader = true))
    
    true
  }
}
