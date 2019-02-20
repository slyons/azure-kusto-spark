package com.microsoft.kusto.spark

import com.microsoft.kusto.spark.datasource.KustoOptions
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.junit.runner.RunWith
import org.scalamock.scalatest.MockFactory
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}
import com.microsoft.kusto.spark.datasource.KustoFilter
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

@RunWith(classOf[JUnitRunner])
class KustoFilterTests extends FlatSpec with MockFactory with Matchers{

  private val schema: StructType = StructType(Seq(
    StructField("string", StringType),
    StructField("bool", BooleanType),
    StructField("int", IntegerType),
    StructField("byte", ByteType),
    StructField("double", DoubleType),
    StructField("float", FloatType),
    StructField("long", LongType),
    StructField("short", ShortType),
    StructField("date", DateType),
    StructField("timestamp", TimestampType)))

  val unknownFilter = new Filter {
    override def references: Array[String] = Array("UnknownFilter")
  }

  "Filter expression" should "be empty if filters list is empty" in {
    assert(KustoFilter.buildFiltersClause(StructType(Nil), Seq.empty) === "")
  }

  "Filter expression" should "be empty if filter is unknown" in {
    assert(KustoFilter.buildFiltersClause(StructType(Nil), Seq(unknownFilter)) === "")
  }
}
