package com.tresata.spark.scalding

import java.nio.ByteBuffer
import scala.reflect.ClassTag
import org.apache.spark.{ SparkEnv, Partitioner, Partition, TaskContext }
import org.apache.spark.rdd.ShuffledRDD

import org.apache.spark.rdd.RDD

private[scalding] object FoldFunctions {
  implicit def rddToFoldFunctions[K, V](rdd: RDD[(K, V)])(implicit kt: ClassTag[K], vt: ClassTag[V], kord: Ordering[K]) =
    new FoldFunctions(rdd)

  // strict ordering because of the fallback on ord
  private class HashOrdering[A](ord: Ordering[A]) extends Ordering[A] {
    override def compare(x: A, y: A): Int = {
      val h1 = if (x == null) 0 else x.hashCode()
      val h2 = if (y == null) 0 else y.hashCode()
      if (h1 < h2) -1 else if (h1 == h2) ord.compare(x, y) else 1
    }
  }

  // not an ordering at all
  private[scalding] class NoOrdering[A] extends Ordering[A] {
    override def compare(x: A, y: A): Int = 0
  }
}

private[scalding] class FoldFunctions[K, V](self: RDD[(K, V)])(implicit kt: ClassTag[K], vt: ClassTag[V], kord: Ordering[K]) 
    extends Serializable {
  import FoldFunctions._

  /**
   * Group the values for each key in the RDD and apply a binary operator to a start value and all 
   * ordered values for a key, going left to right.
   * 
   * Note: this operation may be expensive, since there is no map-side combine, so all values are
   * send through the shuffle.
   */
  def foldLeftByKey[U: ClassTag](valueOrdering: Ordering[V], zeroValue: U, partitioner: Partitioner)(func: (U, V) => U): RDD[(K, U)] = {
    val keyPartitioner = new Partitioner{
      override def numPartitions: Int = partitioner.numPartitions

      override def getPartition(key: Any): Int = partitioner.getPartition(key.asInstanceOf[Tuple2[Any, Any]]._1)
    }

    val shuffled = new ShuffledRDD[(K, V), Unit, Unit](self.map{ kv => (kv, ())}, keyPartitioner)
      .setKeyOrdering(Ordering.Tuple2(new HashOrdering(kord), valueOrdering))

    val zeroBuffer = SparkEnv.get.closureSerializer.newInstance().serialize(zeroValue)
    val zeroArray = new Array[Byte](zeroBuffer.limit)
    zeroBuffer.get(zeroArray)
    lazy val cachedSerializer = SparkEnv.get.closureSerializer.newInstance()
    val createZero = () => cachedSerializer.deserialize[U](ByteBuffer.wrap(zeroArray))

    new RDD[(K, U)](shuffled) {
      def compute(split: Partition, context: TaskContext): Iterator[(K, U)] = new Iterator[(K, U)] {
        private val iter = shuffled.compute(split, context).map(_._1).buffered

        override def hasNext: Boolean = iter.hasNext

        override def next(): (K, U) = {
          val key = iter.head._1
          var u = createZero()
          while (iter.hasNext && iter.head._1 == key)
            u = func(u, iter.next()._2)
          (key, u)
        }
      }

      protected def getPartitions: Array[Partition] = shuffled.getPartitions
    }
  }
}
