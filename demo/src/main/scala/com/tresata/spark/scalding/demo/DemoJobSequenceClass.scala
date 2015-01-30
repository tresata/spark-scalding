package com.tresata.spark.scalding.demo

import com.tresata.spark.scalding.SparkJob
import com.twitter.scalding.{Args, Csv, SequenceFile}

/**
 * Created by Ivan Nikolov <nikolovivann@gmail.com> on 30/01/15.
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
