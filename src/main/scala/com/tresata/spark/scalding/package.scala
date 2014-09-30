package com.tresata.spark.scalding

import scala.collection.JavaConverters._
import com.typesafe.config.Config
import org.apache.hadoop.mapred.{ JobConf, RecordReader, OutputCollector }
import cascading.tap.Tap
import org.apache.spark.{ SparkConf, SparkContext }

object `package` {
  type HadoopTap = Tap[JobConf, RecordReader[_, _], OutputCollector[_, _]]

  def newSc(config: Config, appName: String, moreJars: Option[Seq[String]] = None): SparkContext = {
    val sparkConf = new SparkConf()
      .setAppName(appName)
      .setAll((if (config.hasPath("spark")) config.getConfig("spark").entrySet.asScala.toList else List.empty)
        .map{ entry => ("spark." + entry.getKey.replaceFirst("\\.\"_\"$", ""), entry.getValue.unwrapped.toString) })
    moreJars.foreach{ moreJars =>
      val jars = sparkConf.getOption("spark.jars").map(_.split(",").toSeq).getOrElse(Seq.empty)
      sparkConf.set("spark.jars", (jars ++ moreJars).mkString(","))
    }
    new SparkContext(sparkConf)
  }
}
