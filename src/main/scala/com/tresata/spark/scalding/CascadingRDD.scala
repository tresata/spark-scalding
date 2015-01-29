package com.tresata.spark.scalding

import java.util.Date
import java.io.IOException
import java.text.SimpleDateFormat
import scala.reflect.ClassTag
import scala.collection.JavaConverters._

import org.apache.hadoop.conf.{ Configuration, Configurable }
import org.apache.hadoop.mapred.{ InputFormat, InputSplit, JobConf, Reporter, JobID, TaskAttemptID, TaskID, TaskAttemptContext, JobContext }

import cascading.tuple.{ Tuple => CTuple, TupleEntry, Fields }
import cascading.tap.Tap
import cascading.flow.hadoop.HadoopFlowProcess

import com.twitter.scalding.{ Source, Hdfs, Read, Write, TupleConverter, TupleSetter }

import org.apache.spark.{ Partition, SerializableWritable, SparkContext, TaskContext }
import org.apache.spark.rdd.RDD
import org.apache.spark.util.TaskCompletionListener

class RichSource(val source: Source) extends AnyVal {
  def spark(implicit sc: SparkContext): FieldsRDD = CascadingRDD(sc, source, sc.hadoopConfiguration)
}

class RichRDD[T](val rdd: RDD[T]) extends AnyVal {
  def fieldsRDD(fields: Fields)(implicit ct: ClassTag[T], setter: TupleSetter[T]): FieldsRDD = FieldsRDD(fields)(rdd)
}

object Dsl {
  // these methods have crazy names to avoid any import clashes
  implicit def sourceToSparkRichSource(source: Source): RichSource = new RichSource(source)
  implicit def rddToFieldsRichRDD[T](rdd: RDD[T]): RichRDD[T] = new RichRDD(rdd)
}

object FieldsRDD {
  private val ctupleCt = implicitly[ClassTag[CTuple]]

  def apply[T](fields: Fields)(rdd: RDD[T])(implicit ct: ClassTag[T], setter: TupleSetter[T]): FieldsRDD = {
    val isCTuple = ct == ctupleCt
    val fields1 = fields
    new RDD(rdd)(ctupleCt) with FieldsRDD {
      override def fields: Fields = fields1

      override def compute(split: Partition, context: TaskContext): Iterator[CTuple] = if (isCTuple)
        firstParent[CTuple].iterator(split, context)
      else
        firstParent[T].iterator(split, context).map(setter.apply)

      override protected def getPartitions: Array[Partition] = firstParent[CTuple].partitions
    }
  }
}

trait FieldsRDD extends RDD[CTuple] {
  def fields: Fields

  def tupleEntries: RDD[TupleEntry] = map{ ctuple => new TupleEntry(fields, ctuple) }

  def typed[T: ClassTag](fields: Fields = this.fields)(implicit conv : TupleConverter[T]) : RDD[T] = {
    conv.assertArityMatches(fields)
    map{ ctuple => conv(new TupleEntry(fields, ctuple.get(this.fields, fields))) }
  }

  def fieldsApi: FieldsApi = new FieldsApi(fields, this)
}

private class CascadingPartition(rddId: Int, val index: Int, @transient s: InputSplit) extends Partition {
  val inputSplit = new SerializableWritable[InputSplit](s)

  override def hashCode(): Int = 41 * (41 + rddId) + index
}

object CascadingRDD {
  private def firstAvailableClass(first: String, second: String): Class[_] =
    try(Class.forName(first)) catch {
      case e: ClassNotFoundException => Class.forName(second)
    }

  private def newJobContext(job: JobConf, jobId: JobID): JobContext = {
    val klass = firstAvailableClass("org.apache.hadoop.mapred.JobContextImpl", "org.apache.hadoop.mapred.JobContext")
    val ctor = klass.getDeclaredConstructor(classOf[JobConf], classOf[org.apache.hadoop.mapreduce.JobID])
    ctor.setAccessible(true)
    ctor.newInstance(job, jobId).asInstanceOf[JobContext]
  }

  private def newTaskAttemptContext(job: JobConf, attemptId: TaskAttemptID): TaskAttemptContext = {
    val klass = firstAvailableClass("org.apache.hadoop.mapred.TaskAttemptContextImpl", "org.apache.hadoop.mapred.TaskAttemptContext")
    val ctor = klass.getDeclaredConstructor(classOf[JobConf], classOf[TaskAttemptID])
    ctor.setAccessible(true)
    ctor.newInstance(job, attemptId).asInstanceOf[TaskAttemptContext]
  }

  private def taskAttemptContext(job: JobConf, jobTrackerId: String, jobId: Int, splitId: Int, attemptId: Int): TaskAttemptContext = {
    val jobID = new JobID(jobTrackerId, jobId)
    val taId = new TaskAttemptID(new TaskID(jobID, true, splitId), attemptId)
    newTaskAttemptContext(job, taId)
  }

  private def addLocalConfiguration(job: JobConf, jobTrackerId: String, jobId: Int, splitId: Int, attemptId: Int){
    val jobID = new JobID(jobTrackerId, jobId)
    val taId = new TaskAttemptID(new TaskID(jobID, true, splitId), attemptId)
    job.set("mapred.tip.id", taId.getTaskID.toString)
    job.set("mapred.task.id", taId.toString)
    job.setBoolean("mapred.task.is.map", true)
    job.setInt("mapred.task.partition", splitId)
    job.set("mapred.job.id", jobID.toString)
  }

  private def inputFormat(job: JobConf): InputFormat[_, _] = {
    val inputFormat = job.getInputFormat
    if (inputFormat.isInstanceOf[Configurable])
      inputFormat.asInstanceOf[Configurable].setConf(job)
    inputFormat
  }

  def saveToTap(rdd: RDD[CTuple], tap: HadoopTap, fields: Fields = Fields.UNKNOWN , conf: Configuration = new Configuration) {
    val serializedJob = new SerializableWritable(new JobConf(conf))
    def job = serializedJob.value
    val createTime = new Date
    val sinkFields = (rdd, fields) match {
      case (x: FieldsRDD, Fields.UNKNOWN) => x.fields
      case (_, y) => y
    }

    // for side effects on job and tap
    {
      val flowProcess = new HadoopFlowProcess(job)
      tap.presentSinkFields(flowProcess, sinkFields)
      tap.sinkConfInit(flowProcess, job)
    }
    
    def jobTrackerId = new SimpleDateFormat("yyyyMMddHHmm").format(createTime)

    def jobContext = newJobContext(job, new JobID(jobTrackerId, 0))

    def writeToFile(context: TaskContext, iter: Iterator[CTuple]) {
      val localJob = new JobConf(serializedJob.value) // copy
      val splitId = context.partitionId
      val attemptId = (context.attemptId % Int.MaxValue).toInt
      addLocalConfiguration(localJob, jobTrackerId, context.stageId, splitId, attemptId)
      localJob.set("cascading.flow.step", "dontdeleteme")
      val taContext = taskAttemptContext(localJob, jobTrackerId, context.stageId, splitId, attemptId)
      val committer = localJob.getOutputCommitter

      committer.setupTask(taContext)
      val teColl = tap.openForWrite(new HadoopFlowProcess(localJob), null)
      try {
        iter.foreach{ ctuple => if (context.isInterrupted) sys.error("interrupted"); teColl.add(ctuple) }
      } finally {
        teColl.close
      }
      if (committer.needsTaskCommit(taContext)) {
        try(committer.commitTask(taContext)) catch {
          case e: IOException => committer.abortTask(taContext)
        }
      }
    }

    job.getOutputCommitter.setupJob(jobContext)
    rdd.context.runJob(rdd, writeToFile _)
    tap.commitResource(job)
    job.getOutputCommitter.commitJob(jobContext)
  }

  def saveToTap(rdd: RDD[TupleEntry], tap: HadoopTap, conf: Configuration) {
    saveToTap(rdd.map(_.getTuple), tap, rdd.first.getFields, conf)
  }

  def saveToTap(rdd: RDD[TupleEntry], tap: HadoopTap) { saveToTap(rdd, tap, new Configuration) }

  def saveToSource(rdd: RDD[CTuple], source: Source, fields: Fields = Fields.UNKNOWN, conf: Configuration = new Configuration) {
    val tap = source.createTap(Write)(new Hdfs(true, conf)).asInstanceOf[HadoopTap]
    saveToTap(rdd, tap, fields, conf)
  }

  def saveToSource(rdd: RDD[TupleEntry], source: Source, conf: Configuration) {
    saveToSource(rdd.map(_.getTuple), source, rdd.first.getFields, conf)
  }

  def saveToSource(rdd: RDD[TupleEntry], source: Source) { saveToSource(rdd, source, new Configuration) }

  def apply(sc: SparkContext, source: Source, conf: Configuration = new Configuration, minPartitions: Int = 1): CascadingRDD = {
    val tap = source.createTap(Read)(new Hdfs(true, conf)).asInstanceOf[HadoopTap]
    new CascadingRDD(sc, tap, conf, minPartitions)
  }
}

class CascadingRDD(sc: SparkContext, tap: HadoopTap, @transient conf: Configuration = new Configuration, minPartitions: Int = 1)
    extends RDD[CTuple](sc, Nil) with FieldsRDD {
  import CascadingRDD._

  private val serializedJob = new SerializableWritable(new JobConf(conf))
  private def job = serializedJob.value
  private val createTime = new Date

  // for side effects on job and tap
  {
    val flowProcess = new HadoopFlowProcess(job)
    tap.retrieveSourceFields(flowProcess)
    tap.sourceConfInit(flowProcess, job)
  }
  def fields = tap.getSourceFields
  
  override def getPartitions: Array[Partition] =
    inputFormat(job)
      .getSplits(job, minPartitions)
      .zipWithIndex
      .map{ case (split, idx) => new CascadingPartition(id, idx, split) }

  override def compute(split: Partition, context: TaskContext): Iterator[CTuple] = {
    val localJob = new JobConf(serializedJob.value) // copy
    addLocalConfiguration(localJob, new SimpleDateFormat("yyyyMMddHHmm").format(createTime), 
      context.stageId, split.index, (context.attemptId % Int.MaxValue).toInt)
    val teIter = tap.openForRead(
      new HadoopFlowProcess(localJob),
      inputFormat(localJob).getRecordReader(split.asInstanceOf[CascadingPartition].inputSplit.value, localJob, Reporter.NULL)
    )
    //context.addOnCompleteCallback{ () => teIter.close }
    context.addTaskCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = teIter.close
    })
    teIter.asScala
      .map{ te => new CTuple(te.getTuple) } // tupleEntryIterator re-uses tuples
      .map{ ctuple => if (context.isInterrupted) sys.error("interrupted"); ctuple } // this should throw TaskKilledException but its private in Spark
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val cascadingSplit = split.asInstanceOf[CascadingPartition]
    cascadingSplit.inputSplit.value.getLocations.filter(_ != "localhost")
  }

  override def checkpoint() {}
}
