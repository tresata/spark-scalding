package com.tresata.spark.scalding.demo

import com.tresata.spark.scalding.SparkJob
import com.twitter.scalding.{Csv, SequenceFile, Args}

/**
 * Created by inikolov on 29/01/15.
 */
class DemoJobSequenceClass(args: Args) extends SparkJob(args) {

  override def run: Boolean = {
    SequenceFile(args("input"), ('fruitLine)).spark.fieldsApi
      .mapTo('fruitLine -> ('name, 'color, 'quantity)) {
      x: FruitLine =>
        (x.name, x.color, x.quantity)
      }
      .discard('color)
      .groupBy('name) {
        _.sum[Int]('quantity).size
      }.write(Csv(args("output"), separator = "|", writeHeader = true))
    true
  }
}
