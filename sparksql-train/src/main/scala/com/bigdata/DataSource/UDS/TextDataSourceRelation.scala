package com.bigdata.DataSource.UDS

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StringType, StructField, StructType}

/**
 * Author : lihuichuan
 * Time   : 2020/4/6
 **/
class TextDataSourceRelation(override val sqlContext: SQLContext,
                             path: String,
                             userSchema: StructType
                            ) extends BaseRelation with TableScan with Logging with Serializable {

  override def schema: StructType = {
    if (userSchema != null) {
      userSchema
    } else {
      StructType(
        StructField("id", LongType, false) ::
          StructField("name", StringType, false) ::
          StructField("gender", StringType, false) ::
          StructField("saler", LongType, false) ::
          StructField("comm", LongType, false) :: Nil
      )
    }
  }


  override def buildScan(): RDD[Row] = {
    logError("this is huichuan custom buildScan...")

    var rdd = sqlContext.sparkContext.wholeTextFiles(path).map(_._2)
    val schemaField = schema.fields

    // rdd + schemaField
    val rows = rdd.map(fileContent => {
      val lines = fileContent.split("\n")
      val data = lines.map(_.split(",").map(x=>x.trim)).toSeq

      val result = data.map(x => x.zipWithIndex.map{
        case  (value, index) => {

          val columnName = schemaField(index).name

          caseTo(if(columnName.equalsIgnoreCase("gender")) {
            if(value == "0") {
              "男"
            } else if(value == "1"){
              "女"
            } else {
              "未知"
            }
          } else {
            value
          }, schemaField(index).dataType)
        }
      })
      result.map(x => Row.fromSeq(x))
    })

    rows.flatMap(x=>x)
  }


  def caseTo(value: String, dataType: DataType) = {
    dataType match {
      case _:DoubleType => value.toDouble
      case _:LongType => value.toLong
      case _:StringType => value
    }
  }
}

