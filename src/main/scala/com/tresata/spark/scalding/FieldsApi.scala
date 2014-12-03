package com.tresata.spark.scalding

import org.slf4j.LoggerFactory

import cascading.tuple.{ Tuple => CTuple, TupleEntry, Fields }

import com.twitter.algebird.{ AveragedValue, Semigroup }
import com.twitter.scalding.Dsl._
import com.twitter.scalding.{ FoldOperations, Source, TupleConverter, TupleSetter }
import com.twitter.scalding.serialization.Externalizer

import org.apache.spark.SparkContext._
import org.apache.spark.Partitioner.defaultPartitioner
import org.apache.spark.rdd.RDD

import com.tresata.spark.scalding.FoldFunctions.rddToFoldFunctions

private object FieldConversions extends com.twitter.scalding.FieldConversions

object FieldsApi {
  implicit private class RichCTuple(val ctuple: CTuple) extends AnyVal {
    def copy: CTuple = new CTuple(ctuple)
    def removeF(declarator: Fields, selector: Fields): CTuple = { ctuple.remove(declarator, selector); ctuple }
    def putF(declarator: Fields, fields: Fields, ctuple: CTuple): CTuple = { this.ctuple.put(declarator, fields, ctuple); this.ctuple }
    def addF(value: Any): CTuple = { this.ctuple.add(value); this.ctuple }
  }
}

class FieldsApi(val fields: Fields, val rdd: RDD[CTuple]) extends Serializable {
  import FieldsApi._

  def fieldsRDD: FieldsRDD = FieldsRDD(fields)(rdd)

  def write(source: Source): Unit = CascadingRDD.saveToSource(rdd, source, fields)

  def thenDo(f: FieldsApi => FieldsApi): FieldsApi = f(this)

  def maybeDo[X](x: Option[X])(f: (FieldsApi, X) => FieldsApi): FieldsApi = x.map(f(this, _)).getOrElse(this)

  def project(fields: Fields): FieldsApi = new FieldsApi(
    this.fields.select(fields),
    rdd.map(_.get(this.fields, fields))
  )

  def rename(fs: (Fields, Fields)): FieldsApi = new FieldsApi(
    fields.rename(fs._1, fs._2),
    rdd
  )

  def discard(fields: Fields): FieldsApi = new FieldsApi(
    this.fields.subtract(fields),
    rdd.map(_.copy.removeF(this.fields, fields))
  )
  
  def map[A, T](fs: (Fields, Fields))(fn: A => T)(implicit conv: TupleConverter[A], setter: TupleSetter[T]): FieldsApi = {
    conv.assertArityMatches(fs._1)
    setter.assertArityMatches(fs._2)

    val lockedFn = Externalizer(fn)
    val f: CTuple => CTuple = ctuple => setter(lockedFn.get(conv(new TupleEntry(fs._1, ctuple.get(fields, fs._1)))))

    FieldConversions.defaultMode(fs._1, fs._2) match {
      case Fields.REPLACE =>
        if (fs._1 == Fields.ALL)
          new FieldsApi(
            fields,
            rdd.map(f)
          )
        else
          new FieldsApi(
            fields,
            rdd.map{ ctuple => ctuple.copy.putF(fields, fs._1, f(ctuple)) }
          )
      case Fields.ALL =>
        new FieldsApi(
          fields.append(fs._2),
          rdd.map{ ctuple => ctuple.append(f(ctuple)) }
        )
      case Fields.SWAP =>
        new FieldsApi(
          fields.subtract(fs._1).append(fs._2),
          rdd.map{ ctuple => ctuple.copy.removeF(fields, fs._1).append(f(ctuple)) }
        )
    }
  }

  def mapTo[A, T](fs: (Fields, Fields))(fn: A => T)(implicit conv: TupleConverter[A], setter: TupleSetter[T]): FieldsApi = {
    conv.assertArityMatches(fs._1)
    setter.assertArityMatches(fs._2)

    val lockedFn = Externalizer(fn)
    val f: CTuple => CTuple = ctuple => setter(lockedFn.get(conv(new TupleEntry(fs._1, ctuple.get(fields, fs._1)))))

    new FieldsApi(
      fs._2,
      rdd.map(f)
    )
  }

  def flatMap[A, T](fs: (Fields, Fields))(fn: A => TraversableOnce[T])(implicit conv: TupleConverter[A], setter: TupleSetter[T]): FieldsApi = {
    conv.assertArityMatches(fs._1)
    setter.assertArityMatches(fs._2)

    val lockedFn = Externalizer(fn)
    val f: CTuple => TraversableOnce[CTuple] = ctuple => lockedFn.get(conv(new TupleEntry(fs._1, ctuple.get(fields, fs._1)))).map(setter.apply)

    FieldConversions.defaultMode(fs._1, fs._2) match {
      case Fields.REPLACE =>
        if (fs._1 == Fields.ALL)
          new FieldsApi(
            fields,
            rdd.flatMap(f)
          )
        else
          new FieldsApi(
            fields,
            rdd.flatMap{ ctuple => f(ctuple).map{ out => ctuple.copy.putF(fields, fs._1, out) } }
          )
      case Fields.ALL =>
        new FieldsApi(
          fields.append(fs._2),
          rdd.flatMap{ ctuple => f(ctuple).map(out => ctuple.append(out)) }
        )
      case Fields.SWAP =>
        new FieldsApi(
          fields.subtract(fs._1).append(fs._2),
          rdd.flatMap{ ctuple => f(ctuple).map{ out => ctuple.copy.removeF(fields, fs._1).append(out) } }
        )
    }
  }

  def flatMapTo[A, T](fs: (Fields, Fields))(fn: A => TraversableOnce[T])(implicit conv: TupleConverter[A], setter: TupleSetter[T]): FieldsApi = {
    conv.assertArityMatches(fs._1)
    setter.assertArityMatches(fs._2)

    val lockedFn = Externalizer(fn)
    val f: CTuple => TraversableOnce[CTuple] = ctuple => lockedFn.get(conv(new TupleEntry(fs._1, ctuple.get(fields, fs._1)))).map(setter.apply)

    new FieldsApi(
      fs._2,
      rdd.flatMap(f)
    )
  }

  def filter[A](fields: Fields)(fn: A => Boolean)(implicit conv: TupleConverter[A]): FieldsApi = {
    conv.assertArityMatches(fields)

    val lockedFn = Externalizer(fn)

    new FieldsApi(
      this.fields,
      rdd.filter{ ctuple => lockedFn.get(conv(new TupleEntry(fields, ctuple.get(this.fields, fields)))) }
    )
  }

  def filterNot[A](fields: Fields)(fn: A => Boolean)(implicit conv: TupleConverter[A]): FieldsApi = filter[A](fields)(!fn(_))

  def flatten[T](fs: (Fields, Fields))(implicit conv: TupleConverter[TraversableOnce[T]], setter: TupleSetter[T]): FieldsApi =
    flatMap[TraversableOnce[T], T](fs)({ it: TraversableOnce[T] => it })(conv, setter)

  def flattenTo[T](fs: (Fields, Fields))(implicit conv: TupleConverter[TraversableOnce[T]], setter: TupleSetter[T]): FieldsApi =
    flatMapTo[TraversableOnce[T], T](fs)({ it: TraversableOnce[T] => it })(conv, setter)

  def insert[A](fields: Fields, value: A)(implicit setter: TupleSetter[A]): FieldsApi = {
    setter.assertArityMatches(fields)

    new FieldsApi(
      this.fields.append(fields),
      rdd.map(_.append(setter(value)))
    )
  }

  def join(fs: (Fields, Fields), other: FieldsApi): FieldsApi = {
    val joinedFields = if (fs._1 == fs._2)
      fields.append(other.fields.subtract(fs._1))
    else
      fields.append(other.fields)
    val pairRDD = rdd.map{ ctuple => (ctuple.get(fields, fs._1), ctuple) }
    val otherPairRDD = other.rdd.map{ ctuple => (ctuple.get(other.fields, fs._2), ctuple) }
    val joinedRDD = if (fs._1 == fs._2)
      pairRDD.join(otherPairRDD).map{ case (k, (ctuple1, ctuple2)) => ctuple1.append(ctuple2.copy.removeF(other.fields, fs._2)) }
    else
      pairRDD.join(otherPairRDD).map{ case (k, (ctuple1, ctuple2)) =>  ctuple1.append(ctuple2) }
    new FieldsApi(joinedFields, joinedRDD)
  }

  def join(fields: Fields, other: FieldsApi): FieldsApi = join((fields, fields), other)
  
  def leftJoin(fs: (Fields, Fields), other: FieldsApi): FieldsApi = new FieldsApi(
    fields.append(other.fields),
    rdd
      .map{ ctuple => (ctuple.get(fields, fs._1), ctuple) }
      .leftOuterJoin(other.rdd.map{ ctuple => (ctuple.get(other.fields, fs._2), ctuple) })
      .map{
        case (k, (ctuple1, Some(ctuple2))) => ctuple1.append(ctuple2)
        case (k, (ctuple1, None)) => ctuple1.append(CTuple.size(other.fields.size))
      }
  )

  def groupBy(groupFields: Fields)(builder: GroupBuilder => GroupBuilder): FieldsApi =
    builder(GroupBuilder.apply).schedule(groupFields, this)

  def unpivot(fs: (Fields, Fields)): FieldsApi = {
    assert(fs._2.size == 2, "must specify two results")
    flatMap(fs){ te: TupleEntry => TupleConverter.KeyValueList(te) }.discard(fs._1)
  }

  def name(name: String): FieldsApi = { rdd.setName(name); this }

  def distinct(fields: Fields): FieldsApi = {
    val tmp = project(fields)
    new FieldsApi(tmp.fields, tmp.rdd.distinct)
  }

  def unique(fields: Fields): FieldsApi = distinct(fields)

  def ++(that: FieldsApi): FieldsApi = {
    require(fields == that.fields, "fields must be same for ++ operation")
    new FieldsApi(fields, rdd.union(that.rdd))
  }

  def sample(percent: Double): FieldsApi = new FieldsApi(fields, rdd.sample(false, percent))
  def sample(percent: Double, seed: Long): FieldsApi = new FieldsApi(fields, rdd.sample(false, percent, seed))

  def sampleWithReplacement(fraction: Double): FieldsApi = new FieldsApi(fields, rdd.sample(true, fraction))
  def sampleWithReplacement(fraction: Double, seed: Long): FieldsApi = new FieldsApi(fields, rdd.sample(true, fraction, seed))

  def debug: FieldsApi = new FieldsApi(fields, rdd.map{ x => println(x); x })

  // the following are not taken from scalding's RichPipe but from spark's RDD

  def cache(): FieldsApi = { rdd.cache(); this }

  def unpersist(blocking: Boolean = true): FieldsApi = { rdd.unpersist(blocking); this }

  def insertUniqueId(fields: Fields) = {
    assert(fields.size == 1, "fields must be size 1")

    new FieldsApi(
      this.fields.append(fields),
      rdd.zipWithUniqueId.map{ case (ctuple, id) => ctuple.copy.addF(id) }
    )
  }
}

object GroupBuilder {
  private val log = LoggerFactory.getLogger(getClass)

  private sealed trait ReduceOperation {
    def argsFields: Fields
    def resultFields: Fields

    // act like a folder
    def getA: Any
    def foldV(acc: Any, entry: TupleEntry): Any
    def mapA(a: Any): CTuple
  }

  private class Combiner(
    fs: (Fields, Fields),
    @transient createCombiner: Any => Any, @transient mergeValue: (Any, Any) => Any,
    @transient mergeCombiners: (Any, Any) => Any, @transient mapCombiner: Any => Any,
    conv: TupleConverter[Any],
    setter: TupleSetter[Any]
  ) extends ReduceOperation with Serializable {

    val createCombinerLocked = Externalizer(createCombiner)
    val mergeValueLocked = Externalizer(mergeValue)
    val mergeCombinersLocked = Externalizer(mergeCombiners)
    val mapCombinerLocked = Externalizer(mapCombiner)

    def argsFields: Fields = fs._1

    def resultFields: Fields = fs._2

    // this is combiner api

    def createC(entry: TupleEntry): Any = createCombinerLocked.get(conv(entry.selectEntry(fs._1)))

    def mergeV(c: Any, entry: TupleEntry): Any = mergeValueLocked.get(c, conv(entry.selectEntry(fs._1)))

    def mergeC(c1: Any, c2: Any): Any = mergeCombinersLocked.get(c1, c2)

    def mapC(c: Any): CTuple = setter(mapCombinerLocked.get(c))

    // this is folder api

    def getA: Any = null

    def foldV(acc: Any, entry: TupleEntry): Any = acc match {
      case null => createC(entry)
      case c => mergeV(c, entry)
    }

    def mapA(a: Any): CTuple = mapC(a)
  }

  private class Folder(
    fs: (Fields, Fields),
    @transient init: Any,
    @transient foldValue: (Any, Any) => Any,
    conv: TupleConverter[Any],
    setter: TupleSetter[Any]
  ) extends ReduceOperation with Serializable {

    val initLocked = Externalizer(init)
    val foldValueLocked = Externalizer(foldValue)

    def argsFields: Fields = fs._1

    def resultFields: Fields = fs._2

    def getA: Any = initLocked.get

    def foldV(acc: Any, entry: TupleEntry): Any = foldValueLocked.get(acc, conv(entry.selectEntry(fs._1)))

    def mapA(a: Any): CTuple = setter(a)
  }

  private[scalding] def apply(): GroupBuilder = new GroupBuilder(List.empty, None)
}

class GroupBuilder private (reverseOperations: List[GroupBuilder.ReduceOperation], sortFields: Option[Fields])
    extends FoldOperations[GroupBuilder] with Serializable {
  import GroupBuilder._

  private lazy val operations = reverseOperations.reverse

  private def combiners = operations.collect{
    case combiner: Combiner => combiner
  }

  private lazy val argsFields: Fields = Fields.merge(operations.map(_.argsFields): _*) // order does not matter and dupes are ok
  private lazy val resultFields: Fields = Fields.join(operations.map(_.resultFields): _*) // order matters and dupes are not ok
  private lazy val projectFields: Fields = Fields.merge(argsFields, sortFields.getOrElse(Fields.NONE)) // order does not matter and dupes are ok

  def combine[V, C, T](fs: (Fields, Fields))(createCombiner: V => C, mergeValue: (C, V) => C, mergeCombiners: (C, C) => C, mapCombiner: C => T)(
    implicit conv: TupleConverter[V], setter: TupleSetter[T]
  ): GroupBuilder = new GroupBuilder(new Combiner(
    fs,
    createCombiner.asInstanceOf[Any => Any], mergeValue.asInstanceOf[(Any, Any) => Any],
    mergeCombiners.asInstanceOf[(Any, Any) => Any], mapCombiner.asInstanceOf[Any => Any],
    conv.asInstanceOf[TupleConverter[Any]], setter.asInstanceOf[TupleSetter[Any]]
  ) :: reverseOperations, sortFields)

  def mapReduceMap[V, C, T](fs: (Fields, Fields))(mapfn: V => C)(redfn: (C, C) => C)(mapfn2: C => T)(implicit startConv: TupleConverter[V],
    middleSetter: TupleSetter[C], middleConv: TupleConverter[C], endSetter: TupleSetter[T]): GroupBuilder =
    combine(fs)(mapfn, (c: C, v: V) => redfn(c, mapfn(v)), redfn, mapfn2)

  def foldLeft[X, T](fs: (Fields, Fields))(init: X)(fn: (X, T) => X)(implicit setter: TupleSetter[X], conv: TupleConverter[T]): GroupBuilder = 
    new GroupBuilder(new Folder(
      fs,
      init.asInstanceOf[Any], fn.asInstanceOf[(Any, Any) => Any],
      conv.asInstanceOf[TupleConverter[Any]], setter.asInstanceOf[TupleSetter[Any]]
    ) :: reverseOperations, sortFields)

  def sortBy(f: Fields): GroupBuilder = new GroupBuilder(
    reverseOperations,
    sortFields match {
      case None => Some(f)
      case Some(fields) => Some(fields.append(f))
    }
  )

  def sorting: Option[Fields] = sortFields

  private def valueOrdering: Ordering[CTuple] = {
    sorting match {
      case Some(sortFields) =>
        new Ordering[CTuple] {
          override def compare(x: CTuple, y: CTuple): Int = x.get(projectFields, sortFields).compareTo(y.get(projectFields, sortFields))
        }
      case None => new FoldFunctions.NoOrdering[CTuple]
    }
  }

  private[scalding] def schedule(groupFields: Fields, fieldsApi: FieldsApi): FieldsApi = {
    val fields = fieldsApi.fields
    val rdd = fieldsApi.rdd
    val isFold = operations.foldLeft(false){
      case (true, _) => true
      case (_, _: Folder) => true
      case _ => false
    } || sortFields.isDefined

    if (isFold) {
      log.info("performing foldLeftByKey operation")
      new FieldsApi(
        groupFields.append(resultFields),
        rdd
          .map{ ctuple => (ctuple.get(fields, groupFields), ctuple.get(fields, projectFields)) }
          .foldLeftByKey(valueOrdering, getA, defaultPartitioner(rdd))(foldV(projectFields))
          .map{ case (group, results) => group.append(mapA(results)) }
      )
    } else {
      log.info("performing combineByKey operation")
      new FieldsApi(
        groupFields.append(resultFields),
        rdd
          .map{ ctuple => (ctuple.get(fields, groupFields), ctuple.get(fields, argsFields)) }
          .combineByKey(createC(argsFields), mergeV(argsFields), mergeC)
          .map{ case (group, results) => group.append(mapC(results)) }
      )
    }
  }

  // used in combineByKey

  private def createC(fields: Fields): CTuple => List[Any] =
    ctuple => combiners.map(_.createC(new TupleEntry(fields, ctuple)))

  private def mergeV(fields: Fields): (List[Any], CTuple) => List[Any] =
    (l, ctuple) => combiners.zip(l).map{ case (combiner, c) => combiner.mergeV(c, new TupleEntry(fields, ctuple)) }

  private def mergeC: (List[Any], List[Any]) => List[Any] =
    (l1, l2) => combiners.zip(l1.zip(l2)).map{ case (combiner, (c1, c2)) => combiner.mergeC(c1, c2) }

  private def mapC: List[Any] => CTuple =
    l => CTuple.size(0).append(combiners.zip(l).map{ case (combiner, c) => combiner.mapC(c) }: _*)

  // used in foldLeftByKey

  private def getA: List[Any] =
    operations.map(_.getA)

  private def foldV(fields: Fields): (List[Any], CTuple) => List[Any] = 
    (l, ctuple) => operations.zip(l).map{ case (operation, acc) => operation.foldV(acc, new TupleEntry(fields, ctuple)) }

  private def mapA: List[Any] => CTuple =
    l => CTuple.size(0).append(operations.zip(l).map{ case (operation, acc) => operation.mapA(acc) }: _*)
}
