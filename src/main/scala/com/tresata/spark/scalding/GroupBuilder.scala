package com.tresata.spark.scalding

import org.slf4j.LoggerFactory

import cascading.tuple.{ Tuple => CTuple, TupleEntry, Fields }

import com.twitter.scalding.{ FoldOperations, StreamOperations, TupleConverter, TupleSetter }
import com.twitter.scalding.serialization.Externalizer

import com.tresata.spark.sorted.PairRDDFunctions._

object GroupBuilder {
  private val log = LoggerFactory.getLogger(getClass)

  private implicit def ctupleOrdering: Ordering[CTuple] = new Ordering[CTuple] {
    override def compare(x: CTuple, y: CTuple): Int = x.compareTo(y)
  }

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
    // since i combiner also needs to be able to act like a folder

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

  private class StreamOperation(fs: (Fields, Fields), @transient mapfn: Iterator[Any] => TraversableOnce[Any], conv: TupleConverter[Any], setter: TupleSetter[Any])
      extends Serializable {

    val mapfnLocked = Externalizer(mapfn)

    def argsFields: Fields = fs._1

    def resultFields: Fields = fs._2

    def f(fields: Fields): Iterator[CTuple] => Iterator[CTuple] =
      it => mapfnLocked.get(it.map(ctuple => conv(new TupleEntry(fs._1, ctuple.get(fields, fs._1))))).toIterator.map(setter.apply)
  }

  private[scalding] def apply(): GroupBuilder = new GroupBuilder(List.empty, None, None)
}

class GroupBuilder private (reverseOperations: List[GroupBuilder.ReduceOperation], sortFields: Option[Fields], 
  streamOperation: Option[GroupBuilder.StreamOperation])
    extends FoldOperations[GroupBuilder] with StreamOperations[GroupBuilder] with Serializable {
  import GroupBuilder._

  private lazy val operations = reverseOperations.reverse

  private def combiners = operations.collect{
    case combiner: Combiner => combiner
  }

  private lazy val argsFields: Fields = streamOperation.map{ op => op.argsFields }.getOrElse(
    Fields.merge(operations.map(_.argsFields): _*) // order does not matter and dupes are ok
  )
  private lazy val resultFields: Fields = streamOperation.map{ op => op.resultFields }.getOrElse(
    Fields.join(operations.map(_.resultFields): _*) // order matters and dupes are not ok
  )
  private lazy val projectFields: Fields = Fields.merge(argsFields, sortFields.getOrElse(Fields.NONE)) // order does not matter and dupes are ok

  def combine[V, C, T](fs: (Fields, Fields))(createCombiner: V => C, mergeValue: (C, V) => C, mergeCombiners: (C, C) => C, mapCombiner: C => T)(
    implicit conv: TupleConverter[V], setter: TupleSetter[T]
  ): GroupBuilder = new GroupBuilder(new Combiner(
    fs,
    createCombiner.asInstanceOf[Any => Any], mergeValue.asInstanceOf[(Any, Any) => Any],
    mergeCombiners.asInstanceOf[(Any, Any) => Any], mapCombiner.asInstanceOf[Any => Any],
    conv.asInstanceOf[TupleConverter[Any]], setter.asInstanceOf[TupleSetter[Any]]
  ) :: reverseOperations, sortFields, streamOperation)

  def mapReduceMap[V, C, T](fs: (Fields, Fields))(mapfn: V => C)(redfn: (C, C) => C)(mapfn2: C => T)(implicit startConv: TupleConverter[V],
    middleSetter: TupleSetter[C], middleConv: TupleConverter[C], endSetter: TupleSetter[T]): GroupBuilder =
    combine(fs)(mapfn, (c: C, v: V) => redfn(c, mapfn(v)), redfn, mapfn2)

  def foldLeft[X, T](fs: (Fields, Fields))(init: X)(fn: (X, T) => X)(implicit setter: TupleSetter[X], conv: TupleConverter[T]): GroupBuilder = 
    new GroupBuilder(new Folder(
      fs,
      init.asInstanceOf[Any], fn.asInstanceOf[(Any, Any) => Any],
      conv.asInstanceOf[TupleConverter[Any]], setter.asInstanceOf[TupleSetter[Any]]
    ) :: reverseOperations, sortFields, streamOperation)

  def sortBy(f: Fields): GroupBuilder = new GroupBuilder(
    reverseOperations,
    sortFields match {
      case None => Some(f)
      case Some(fields) => Some(fields.append(f))
    },
    streamOperation
  )

  def sorting: Option[Fields] = sortFields

  def mapStream[T, X](fs: (Fields, Fields))(mapfn: (Iterator[T]) => TraversableOnce[X])(implicit conv: TupleConverter[T], setter: TupleSetter[X]): GroupBuilder =
    new GroupBuilder(reverseOperations, sortFields, Some(new StreamOperation(
      fs,
      mapfn.asInstanceOf[Iterator[Any] => Iterator[Any]],
      conv.asInstanceOf[TupleConverter[Any]],
      setter.asInstanceOf[TupleSetter[Any]])))

  private def valueOrdering: Option[Ordering[CTuple]] = sorting.map{ sortFields =>
    new Ordering[CTuple] {
      override def compare(x: CTuple, y: CTuple): Int = x.get(projectFields, sortFields).compareTo(y.get(projectFields, sortFields))
    }
  }

  private[scalding] def schedule(groupFields: Fields, fieldsApi: FieldsApi): FieldsApi = {
    val fields = fieldsApi.fields
    val rdd = fieldsApi.rdd

    streamOperation.map{ operation =>
      log.info("performing mapStreamByKey operation")
      require(operations.isEmpty, "mapStreamByKey cannot be combined with other operations")
      new FieldsApi(
        groupFields.append(operation.resultFields),
        rdd
          .map{ ctuple => (ctuple.get(fields, groupFields), ctuple.get(fields, projectFields)) }
          .groupSort(valueOrdering)
          .mapStreamByKey(operation.f(projectFields))
          .map{ case (group, results) => group.append(results) }
      )
    }.getOrElse{
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
            .groupSort(valueOrdering)
            .foldLeftByKey(getA)(foldV(projectFields))
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
