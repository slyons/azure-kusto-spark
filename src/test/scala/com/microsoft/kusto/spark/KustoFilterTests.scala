package com.microsoft.kusto.spark

import java.sql.{Date, Timestamp}

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

  "Filter clause" should "be empty if filters list is empty" in {
    assert(KustoFilter.buildFiltersClause(StructType(Nil), Seq.empty) === "")
  }

  "Filter clause" should "be empty if filter is unknown" in {
    assert(KustoFilter.buildFiltersClause(StructType(Nil), Seq(unknownFilter)) === "")
  }

  "EqualTo expression" should "construct equality filter correctly for string type" in {
    val filter = KustoFilter.buildFilterExpression(schema, EqualTo("string", "abc"))
    filter shouldBe Some("""string == 'abc'""")
  }

  "EqualTo expression" should "construct equality filter correctly for string with tags" in {
    val filter = KustoFilter.buildFilterExpression(schema, EqualTo("string", "'abc'"))
    filter shouldBe Some("""string == '\'abc\''""")
  }

  "EqualTo expression" should "construct equality filter correctly for date type" in {
      // Java.sql.date  year is 1900-based, month is 0-based
      val filter = KustoFilter.buildFilterExpression(schema, EqualTo("date", new Date(119, 1, 21)))
      filter shouldBe Some("""date == datetime('2019-02-21')""")
  }

  "EqualTo expression" should "construct equality filter correctly for timestamp type" in {
    // Java.sql.date  year is 1900-based, month is 0-based
    val filter = KustoFilter.buildFilterExpression(schema, EqualTo("timestamp", new Timestamp(119, 1, 21, 12, 30, 2, 123)))
    filter shouldBe Some("""timestamp == datetime('2019-02-21 12:30:02.000000123')""")
  }

  "EqualTo expression" should "construct equality filter correctly for double type" in {
    val filter = KustoFilter.buildFilterExpression(schema, EqualTo("double", 0.13))
    filter shouldBe Some("""double == 0.13""")
  }

  "EqualNullSafe expression" should "translate to isnull when value is null" in {
    val filter = KustoFilter.buildFilterExpression(schema, EqualNullSafe("string", null))
    filter shouldBe Some("""isnull(string)""")
  }

  "EqualNullSafe expression" should "translate to equality when value is not null" in {
    val filter = KustoFilter.buildFilterExpression(schema, EqualNullSafe("string", "abc"))
    filter shouldBe Some("""string == 'abc'""")
  }

  "GreaterThan expression" should "construct filter expression correctly for byte type" in {
    val filter = KustoFilter.buildFilterExpression(schema, GreaterThan("byte", 5))
    filter shouldBe Some("""byte > 5""")
  }

  "GreaterThanOrEqual expression" should "construct filter expression correctly for float type" in {
    val filter = KustoFilter.buildFilterExpression(schema, GreaterThanOrEqual("float", 123.456))
    filter shouldBe Some("""float >= 123.456""")
  }

  "LessThan expression" should "construct filter expression correctly for byte type" in {
    val filter = KustoFilter.buildFilterExpression(schema, LessThan("byte", 5))
    filter shouldBe Some("""byte < 5""")
  }

  "LessThanOrEqual expression" should "construct filter expression correctly for float type" in {
    val filter = KustoFilter.buildFilterExpression(schema, LessThanOrEqual("float", 123.456))
    filter shouldBe Some("""float <= 123.456""")
  }

  "In expression" should "construct filter expression correctly for a set of values" in {
    val stringArray = Array("One Mississippi", "Two Mississippi", "Hippo")
    val filter = KustoFilter.buildFilterExpression(schema, In("string", stringArray.asInstanceOf[Array[Any]]))
    filter shouldBe Some("""string in ('One Mississippi', 'Two Mississippi', 'Hippo')""")
  }

  "IsNull expression" should "construct filter expression correctly" in {
    val filter = KustoFilter.buildFilterExpression(schema, IsNull("byte"))
    filter shouldBe Some("""isnull(byte)""")
  }

  "IsNotNull expression" should "construct filter expression correctly" in {
    val filter = KustoFilter.buildFilterExpression(schema, IsNotNull("byte"))
    filter shouldBe Some("""isnotnull(byte)""")
  }

  "And expression" should "construct inner filters and than construct the and expression" in {
    val leftFilter = IsNotNull("byte")
    val rightFilter = LessThan("float", 5)

    val filter = KustoFilter.buildFilterExpression(schema, And(leftFilter, rightFilter))
    filter shouldBe Some("""(isnotnull(byte)) and (float < 5)""")
  }

  "Or expression" should "construct inner filters and than construct the or expression" in {
    val leftFilter = IsNotNull("byte")
    val rightFilter = LessThan("float", 5)

    val filter = KustoFilter.buildFilterExpression(schema, Or(leftFilter, rightFilter))
    filter shouldBe Some("""(isnotnull(byte)) or (float < 5)""")
  }

  "Not expression" should "construct the child filter and than construct the not expression" in {
    val childFilter = IsNotNull("byte")

    val filter = KustoFilter.buildFilterExpression(schema, Not(childFilter))
    filter shouldBe Some("""not(isnotnull(byte))""")
  }

  "StringStartsWith expression" should "construct the correct expression" in {
    val filter = KustoFilter.buildFilterExpression(schema, StringStartsWith("string", "OMG"))
    filter shouldBe Some("""string startswith_cs 'OMG'""")
  }

  "StringEndsWith expression" should "construct the correct expression" in {
    val filter = KustoFilter.buildFilterExpression(schema, StringEndsWith("string", "OMG"))
    filter shouldBe Some("""string endswith_cs 'OMG'""")
  }

  "StringContains expression" should "construct the correct expression" in {
    val filter = KustoFilter.buildFilterExpression(schema, StringContains("string", "OMG"))
    filter shouldBe Some("""string contains_cs 'OMG'""")
  }

}
