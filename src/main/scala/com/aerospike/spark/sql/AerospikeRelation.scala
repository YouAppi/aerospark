package com.aerospike.spark.sql

import scala.collection.JavaConversions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import com.aerospike.client.Value
import com.aerospike.client.query.Statement
import com.typesafe.scalalogging.slf4j.LazyLogging
/**
 * This class infers the schema used by the DataFrame
 * and creates an instance of @see com.aerospike.spark.sql.KeyRecordRDD
 */
class AerospikeRelation(config: AerospikeConfig, userSchema: StructType, typeConverter: TypeConverter)(@transient val sqlContext: SQLContext)
    extends BaseRelation with TableScan with PrunedFilteredScan with LazyLogging with Serializable {

  Value.UseDoubleType = true
  var schemaCache: StructType = _

  override def schema: StructType = {
    if (schemaCache == null || schemaCache.isEmpty) {
      val client = AerospikeConnection.getClient(config)
      val fields = Vector[StructField](
        StructField(config.keyColumn(), StringType, nullable = true),
        StructField(config.digestColumn(), BinaryType, nullable = false),
        StructField(config.expiryColumn(), IntegerType, nullable = false),
        StructField(config.generationColumn(), IntegerType, nullable = false),
        StructField(config.ttlColumn(), IntegerType, nullable = false),
        StructField(config.lutColumn(), LongType, nullable = false)  //fake Last Update Time column (represented in nanoseconds)
      )

      val stmt = new Statement()
      stmt.setNamespace(config.get(AerospikeConfig.NameSpace).asInstanceOf[String])
      stmt.setSetName(config.get(AerospikeConfig.SetName).asInstanceOf[String])

      val recordSet = client.query(null, stmt)
      val bins: Map[String, StructField] = try {
        val sample = recordSet.take(config.schemaScan())
        sample.flatMap { keyRecord =>
          keyRecord.record.bins.map {
            case (binName, binVal) =>
              val field = typeConverter.valueToSchema(binName -> binVal)
              logger.debug(s"Schema - Bin:$binName, Value:$binVal, Field:$field")
              binName -> field
          }
        }.toMap
      } catch {
        case e: Exception =>
          throw e
          //Map.empty[String, StructField]
      } finally {
        recordSet.close()
      }
      schemaCache = StructType(fields ++ bins.values)
    }
    schemaCache
  }

  override def buildScan(): RDD[Row] = {
    new KeyRecordRDD(sqlContext.sparkContext, config, schemaCache, typeConverter = typeConverter)
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    if (filters.length > 0) {
      new KeyRecordRDD(sqlContext.sparkContext, config, schemaCache, requiredColumns, filters, typeConverter = typeConverter)
    } else {
      new KeyRecordRDD(sqlContext.sparkContext, config, schemaCache, requiredColumns, typeConverter = typeConverter)
    }
  }
}
