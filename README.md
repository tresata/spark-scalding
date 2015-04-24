[![Build Status](https://travis-ci.org/tresata/spark-scalding.svg?branch=master)](https://travis-ci.org/tresata/spark-scalding)

# spark-scalding
Spark-scalding is a library that aims to make the transition from Cascading/Scalding to Spark a little easier by adding support for Cascading Taps, Scalding Sources and the Scalding Fields API in Spark.

Note:
* We refer to cascading.tuple.Tuple as CTuple to avoid confusion with scalding tuples (Tuple1, Tuple2, etc.).
* When we say Tap we really mean Tap for Hadoop, so `Tap[JobConf, RecordReader[_, _], OutputCollector[_, _]]`

## CascadingRDD
Cascading has a rich ecosystem that includes many Schemes and Taps to read from (and write to) different data formats (CSV, Avro, etc.) and data stores (HDFS, HBase, RDMBS, etc.). We at Tresata depend on both open-source and in-house proprietary taps for many of our products. See for example our cascading-opencsv library (https://github.com/tresata/cascading-opencsv). Instead of writing RDDs from scratch to read/write in Spark from these data formats and data stores, CascadingRDD allows you to re-use existing Schemes and Taps in Spark. The CascadingRDD is a `RDD[CTuple] { def fields: Fields }` which means you have access to both the metadata (Fields) and data (CTuples) of your Cascading Tap. To write to a Tap you will have to provide an `RDD[CTuple]` and Fields object. Finally, CascadingRDD also supports reading from and writing to scalding Sources.

## FieldsAPI
Although the Typed API is generally considered the future of Scalding, we still prefer the Fields API for ad hoc analysis, when dealing with many (and possibly unknown) columns, or when leveraging the GroupBuilder abstraction for reduce operations. To ease the transition to Spark we ported the Scalding Fields API to Spark as FieldsAPI. It is not a complete port: we might have left out some methods in RichPipe and JoinAlgorithms simply because we haven't gotten to it yet. And although we do support methods from StreamOperations on the GroupBuilder the behavior might be somewhat different from what you would expect (and we have currently no intention of fixing that).
The FieldsAPI is almost indistinguishable from a Scalding Job using the Fields API. The only major difference is that instead of calling `.read` on a Source you call `.spark.fieldsApi` (`.spark` turns it into an `RDD[CTuple]`, `.fieldsApi` turns it into a TypedAPI)

## SparkJob
SparkJob allows you to run your Spark job using Scalding's Tool and Job infrastructure. For most this will perhaps be unnecessary, but if you already use Tool and Job and have many extensions for them in-house then this might come in handy. To use it, write a Job that extends SparkJob, build a fat/assembly jar that includes your job, Scalding, but not Spark (which should be a provided dependency), and you can launch it with spark-submit. Since Spark typically has a lot of settings you want to tweak, SparkJob also supports the Typesafe config (https://github.com/typesafehub/config) so you can provide reasonable defaults for your job in an application.conf (versus reasonable defaults for the cluster which go into spark-defaults.conf for your Spark installation). Any setting in the Typesafe config under spark will be applied to the SparkConf.

We include a small demo project (in demo dir) that shows how to create a fat-jar with a Spark job. It also demonstrates how to use FieldsApi. You should be able to launch a Spark job (assuming of course your cluster is configured properly) like this:
```
$ sbt
> project demo
> assembly
> exit
$ hadoop fs -put test/data/fruits.bsv
$ spark-submit --class com.twitter.scalding.Tool demo/target/scala-2.10/spark-scalding-demo-assembly-0.4.0-SNAPSHOT.jar com.tresata.spark.scalding.demo.DemoJob --hdfs --input fruits.bsv --output out
```
Have fun!
Team @ Tresata
