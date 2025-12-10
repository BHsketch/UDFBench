package com.example.scalaudfs

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.api.java.UDF2
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.Row

class ExtractFromDateScala extends UDF1[String, org.apache.spark.sql.Row] {

  override def call(date:String) : org.apache.spark.sql.Row = {
    
    if (date == null) {
      //(id, null.asInstanceOf[Int], null.asInstanceOf[Int], null.asInstanceOf[Int])
      Row(null, null, null);
    } else {
      val parts = date.split("-")
      if (parts.length == 3)
        Row(parts(0).toInt, parts(1).toInt, parts(2).toInt)
      else
        Row(null, null, null)
    }
  }

}
