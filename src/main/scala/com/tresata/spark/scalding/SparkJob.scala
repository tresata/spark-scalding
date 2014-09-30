package com.tresata.spark.scalding

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import com.twitter.scalding.{ Args, Job, Source, HadoopMode }
import org.apache.spark.{ SparkConf, SparkContext }

class SparkJob(args: Args) extends Job(args) {
  def hadoopConf: Configuration = mode match {
    case hm: HadoopMode => hm.jobConf
    case _ => new Configuration()
  }

  private def tmpJars: Option[Seq[String]] = Option(hadoopConf.get("tmpjars")).map(_.split(",")) // re-use tmpjars in spark

  implicit def sourceToSparkRichSource(source: Source): RichSource = new RichSource(source)

  implicit lazy val sc: SparkContext = newSc(ConfigFactory.load, name, tmpJars)

  override def run : Boolean = true
}
