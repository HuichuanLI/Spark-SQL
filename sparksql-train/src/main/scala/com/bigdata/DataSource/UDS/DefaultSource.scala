package com.bigdata.DataSource.UDS

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

/**
 * Author : lihuichuan
 * Time   : 2020/4/6
 **/
class DefaultSource extends RelationProvider with SchemaRelationProvider{
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext,parameters,null)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    val path = parameters.get("path")

    path match {
      case Some(p)=> new TextDataSourceRelation(sqlContext,p,schema)
      case _=> throw new IllegalArgumentException("path is required ...")
    }


  }
}
