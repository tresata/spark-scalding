package com.tresata.spark.scalding

import com.twitter.scalding.{Source, HadoopMode, Job, Args, Config}
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.serializer.{ Serialization => HSerialization }
import org.apache.spark.{ SparkConf, SparkContext }

abstract class SparkJob(args: Args) extends Job(args) {
  implicit def hadoopConf: Configuration = {
    val conf = mode match {
      case hm: HadoopMode => hm.jobConf
      case _ => new Configuration()
    }
    config.foreach {
      case (key: String, value: String) => conf.set(key, value)
    }
    conf
  }

  private def tmpJars: Option[Seq[String]] = Option(hadoopConf.get("tmpjars")).map(_.split(",")) // re-use tmpjars in spark

  implicit def sourceToSparkRichSource(source: Source): RichSource = new RichSource(source)

  @transient implicit lazy val sc: SparkContext = newSc(ConfigFactory.load, name, tmpJars)

  override def run : Boolean

  /**
   * Overrides the Scalding Job config method in order not to use the
   * default serializations, but to pass different ones instead.
   * 
   * @return
   */
  override def config: Map[AnyRef, AnyRef] = {
    // this will overwrite the io.serializations option to what
    // we need it to be.
    super.config ++ setSerialization(ioSerializations).toMap
  }

  /**
   * Hadoop and Cascading serialization needs to be first, and the Kryo serialization
   * needs to be last and this method handles this for you:
   * hadoop, cascading, [userHadoop,] kyro
   * is the order.
   *
   * Kryo uses a specific class here.
   *
   * @param userHadoop
   * @return
   */
  protected def setSerialization(userHadoop: Seq[Class[_ <: HSerialization[_]]] = Nil): Config = {
    
    // Hadoop and Cascading should come first
    val first: Seq[Class[_ <: HSerialization[_]]] =
      Seq(classOf[org.apache.hadoop.io.serializer.WritableSerialization],
        classOf[cascading.tuple.hadoop.TupleSerialization])
    // this must come last
    val last: Seq[Class[_ <: HSerialization[_]]] = Seq(classOf[com.tresata.spark.scalding.serialization.KryoSerialization])
    val required = (first ++ last).toSet[AnyRef] // Class is invariant, but we use it as a function
    // Make sure we keep the order correct and don't add the required fields twice
    val hadoopSer = first ++ (userHadoop.filterNot(required)) ++ last

    val hadoopKV = Map(Config.IoSerializationsKey -> hadoopSer.map(_.getName).mkString(","))
    
    Config(hadoopKV)
  }  
}
