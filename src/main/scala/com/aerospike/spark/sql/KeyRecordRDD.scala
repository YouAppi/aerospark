package com.aerospike.spark.sql


import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import com.aerospike.client.Value
import com.aerospike.client.query.Statement
import com.aerospike.helper.query._
import com.aerospike.helper.query.Qualifier.FilterOperation
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.MapType

case class AerospikePartition(index: Int, host: String) extends Partition

/**
  * This is an Aerospike specific RDD to contains the results
  * of a Scan or Query operation.
  *
  * NOTE: This class uses the @see com.aerospike.helper.query.QueryEngine to
  * provide multiple filters in the Aerospike server.
  *
  */
class KeyRecordRDD(
                    @transient val sc: SparkContext,
                    val aerospikeConfig: AerospikeConfig,
                    val schema: StructType = null,
                    val requiredColumns: Array[String] = null,
                    val filters: Array[Filter] = null,
                    typeConverter: TypeConverter
                  ) extends RDD[Row](sc, Seq.empty) with LazyLogging {

  override protected def getPartitions: Array[Partition] = {
    val client = AerospikeConnection.getClient(aerospikeConfig)
    val nodes = client.getNodes

    nodes.zipWithIndex.map { case (node, i) =>
      val name = node.getName
      AerospikePartition(i, name).asInstanceOf[Partition]
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {

    val partition: AerospikePartition = split.asInstanceOf[AerospikePartition]
    val queryEngine = AerospikeConnection.getQueryEngine(aerospikeConfig)
    val client = AerospikeConnection.getClient(aerospikeConfig)
    val node = client.getNode(partition.host)

    val stmt = new Statement()
    stmt.setNamespace(aerospikeConfig.namespace())
    stmt.setSetName(aerospikeConfig.set())
    val metaFields = typeConverter.metaFields(aerospikeConfig)


    // set required bins
    Option(requiredColumns)
      .collect { case cols if cols.nonEmpty => typeConverter.binNamesOnly(cols, metaFields) }
      .foreach { binsOnly =>
        logDebug(s"Bin names: $binsOnly")
        stmt.setBinNames(binsOnly: _*)
      }

    val kri: KeyRecordIterator = Option(filters).collect { case fs if fs.nonEmpty => fs.map(filterToQualifier) }
      .map { qs =>
        queryEngine.select(stmt, false, node, qs: _*)
      }.getOrElse(queryEngine.select(stmt, false, node))

    context.addTaskCompletionListener(_ => {
      kri.close()
    })
    new RowIterator(kri, schema, aerospikeConfig, requiredColumns, typeConverter)
  }

  override def getPreferredLocations(split: Partition): Seq[String] =
    Seq(split.asInstanceOf[AerospikePartition].host)


  private def filterToQualifier(filter: Filter): Qualifier = filter match {
    case EqualTo(attribute, value) =>
      if (isList(attribute)) {
        QualifierFactory.create(attribute, FilterOperation.LIST_CONTAINS, value)
      } else if (isMap(attribute)) {
        QualifierFactory.create(attribute, FilterOperation.MAP_KEYS_CONTAINS, value)
      } else {
        QualifierFactory.create(attribute, FilterOperation.EQ, value)
      }
    case GreaterThanOrEqual(attribute, value) =>
      QualifierFactory.create(attribute, FilterOperation.GTEQ, value)

    case GreaterThan(attribute, value) =>
      QualifierFactory.create(attribute, FilterOperation.GT, value)

    case LessThanOrEqual(attribute, value) =>
      QualifierFactory.create(attribute, FilterOperation.LTEQ, value)

    case LessThan(attribute, value) =>
      QualifierFactory.create(attribute, FilterOperation.LT, value)

    case StringStartsWith(attribute, value) =>
      QualifierFactory.create(attribute, FilterOperation.START_WITH, value)

    case StringEndsWith(attribute, value) =>
      QualifierFactory.create(attribute, FilterOperation.ENDS_WITH, value)

    case IsNotNull(attribute) =>
      QualifierFactory.create(attribute, FilterOperation.NOTEQ, Value.getAsNull)

    case And(left, right) =>
      new Qualifier(FilterOperation.AND, filterToQualifier(left), filterToQualifier(right))

    case Or(left, right) =>
      new Qualifier(FilterOperation.OR, filterToQualifier(left), filterToQualifier(right))

    case _ =>
      logger.warn(s"Not matching filter: ${filter.toString}")
      null
  }

  private def isMap(attribute: String) = {
    schema(attribute).dataType match {
      case _: MapType => true
      case _ => false
    }
  }

  private def isList(attribute: String) = {
    schema(attribute).dataType match {
      case _: ArrayType => true
      case _ => false
    }
  }
}

/**
  * This class implement a Spark SQL row iterator.
  * It is used to iterate through the Record/Result set from the Aerospike query
  */
class RowIterator[Row](val kri: KeyRecordIterator,
                       schema: StructType,
                       config: AerospikeConfig,
                       requiredColumns: Array[String] = null,
                       typeConverter: TypeConverter)
  extends Iterator[org.apache.spark.sql.Row] with LazyLogging {


  def hasNext: Boolean = {
    kri.hasNext
  }

  def next: org.apache.spark.sql.Row = {
    val kr = kri.next()
    val digest: Array[Byte] = kr.key.digest
    val digestName: String = config.digestColumn()

    val userKey: Value = kr.key.userKey
    val userKeyName: String = config.keyColumn()

    val expiration: Int = kr.record.expiration
    val expirationName: String = config.expiryColumn()

    val generation: Int = kr.record.generation
    val generationName: String = config.generationColumn()

    val ttl: Int = kr.record.getTimeToLive
    val ttlName: String = config.ttlColumn()

    val lut: Long = System.currentTimeMillis() / 1000 - (config.defaultTTL() * 24 * 60 * 60 - ttl)
    val lutName: String = config.lutColumn()

    val fields = requiredColumns.map { field =>
      val value = field match {
        case x if x.equals(digestName) => digest
        case x if x.equals(userKeyName) => userKey
        case x if x.equals(expirationName) => expiration
        case x if x.equals(generationName) => generation
        case x if x.equals(ttlName) => ttl
        case x if x.equals(lutName) => lut
        case _ => typeConverter.binToValue(schema, (field, kr.record.bins.get(field)))
      }
      logger.debug(s"$field = $value")
      value
    }

    Row.fromSeq(fields)
  }
}
