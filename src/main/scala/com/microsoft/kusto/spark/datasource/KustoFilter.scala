package com.microsoft.kusto.spark.datasource

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

private[kusto] object KustoFilter {
  def pruneSchema(schema: StructType, columns: Array[String]): StructType = {
    val fieldMap = Map(schema.fields.map(x => x.name -> x): _*)
    new StructType(columns.map(name => fieldMap(name)))
  }

  def buildColumnsClause(columns: Array[String]): String = {
    if(columns.isEmpty) "" else {
      " | project " + columns.mkString(", ")
    }
  }

  def buildFiltersClause(schema: StructType, filters: Seq[Filter]): String = {
    val filterExpressions = filters.flatMap(f => buildFilterExpression(schema, f)).mkString(" and ")
    if (filterExpressions.isEmpty) "" else "| where " + filterExpressions
  }

  def buildFilterExpression(schema: StructType, filter: Filter): Option[String] = {

    filter match {
      case EqualTo(attr, value) => buildOperatorFilter(schema, attr, value, "==")
      case EqualNullSafe(attr, value) => ??? // TODO
      case GreaterThan(attr, value) => ??? // TODO
      case GreaterThanOrEqual(attr, value) => ??? // TODO
      case LessThan(attr, value) => ??? // TODO
      case LessThanOrEqual(attr, value) => ??? // TODO
      case In(attr, values) => ??? // TODO
      case IsNull(attr) => ??? // TODO
      case IsNotNull(attr) => ??? // TODO
      case And(left, right) => ??? // TODO
      case Or(left, right) => ??? // TODO
      case Not(child) => ??? // TODO
      case StringStartsWith(attr, value) => ??? // TODO
      case StringEndsWith(attr, value) => ??? // TODO
      case StringContains(attr, value) => ??? // TODO
      case _ => None
    }
  }

  private def buildOperatorFilter(schema: StructType, attr: String, value: Any, operator: String): Option[String] = {
    getType(schema, attr).map {
      dataType => s"$attr $operator ${toStringTagged(value, dataType)}"
    }
  }

  private def toStringTagged(value: Any, dataType: DataType): String = {
    dataType match {
      case StringType => s"'$value'"
      case DateType => s"\\'${value.asInstanceOf[Date]}\\'"
      case TimestampType => s"\\'${value.asInstanceOf[Timestamp]}\\'"
      case _ => s"'${value.toString}'"
    }
  }

  private def getType(schema: StructType, attr: String): Option[DataType] = {
    if (schema.fieldNames.contains(attr)) {
      Some(schema(attr).dataType)
    } else None
  }
}
