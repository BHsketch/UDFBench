package com.example.scalaudfs

//import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
//import org.apache.spark.sql.Row
//import org.apache.spark.sql.types._
//import org.apache.spark.sql.expressions.MutableAggregationBuffer

//object AggregateAvgScala extends UserDefinedAggregateFunction {
  //def inputSchema: StructType = StructType(StructField("value", LongType) :: Nil)
  //def bufferSchema: StructType = StructType(
    //StructField("sum", DoubleType) ::
    //StructField("count", LongType) :: Nil
  //)
  //def dataType: DataType = DoubleType
  //def deterministic: Boolean = true

  //def initialize(buffer: MutableAggregationBuffer): Unit = {
    //buffer(0) = 0.0
    //buffer(1) = 0L
  //}

  //def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //if (!input.isNullAt(0)) {
      //buffer(0) = buffer.getDouble(0) + input.getLong(0)
      //buffer(1) = buffer.getLong(1) + 1
    //}
  //}

  //def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
    //buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  //}

  //def evaluate(buffer: Row): Any = {
    //val cnt = buffer.getLong(1)
    //if (cnt == 0) null else buffer.getDouble(0) / cnt
  //}
//}

//################################### OLD TRY ##########################################
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.functions.udaf

class AggregateAvgScala extends Aggregator[Long, (Double, Long), java.lang.Double] {
  
  // Initial state: (sum, count)
  def zero: (Double, Long) = (0.0, 0L)
  
  // Combine input value with current state
  def reduce(buffer: (Double, Long), value: Long): (Double, Long) = {
    (buffer._1 + value.toDouble, buffer._2 + 1)
  }
  
  // Merge two intermediate states
  def merge(b1: (Double, Long), b2: (Double, Long)): (Double, Long) = {
    (b1._1 + b2._1, b1._2 + b2._2)
  }
  
  // Calculate final result
  def finish(reduction: (Double, Long)): java.lang.Double = {
    if (reduction._2 == 0) {
      null
    } else {
      reduction._1 / reduction._2
    }
  }
  
  // Encoders for serialization
  def bufferEncoder: Encoder[(Double, Long)] = Encoders.product[(Double, Long)]
  //def outputEncoder: Encoder[java.lang.Double] = Encoders.scalaDouble
  def outputEncoder: Encoder[java.lang.Double] = Encoders.DOUBLE
}

//############################### UNSAFE 1 ##################################

//import org.apache.spark.sql.expressions.Aggregator
//import org.apache.spark.sql.{Encoder, Encoders}
//import org.apache.spark.sql.catalyst.InternalRow
//import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
//import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
//import org.apache.spark.sql.types._

//class AggregateAvgScala extends Aggregator[Long, InternalRow, java.lang.Double] {
  
  //// Schema: sum (DoubleType), count (LongType)
  //private val bufferSchema = StructType(Seq(
    //StructField("sum", DoubleType, nullable = false),
    //StructField("count", LongType, nullable = false)
  //))
  
  //def zero: InternalRow = {
    //new GenericInternalRow(Array[Any](0.0, 0L))
  //}
  
  //def reduce(buffer: InternalRow, value: Long): InternalRow = {
    //// Update in place
    //buffer.update(0, buffer.getDouble(0) + value.toDouble)
    //buffer.update(1, buffer.getLong(1) + 1L)
    //buffer
  //}
  
  //def merge(b1: InternalRow, b2: InternalRow): InternalRow = {
    //// Update in place
    //b1.update(0, b1.getDouble(0) + b2.getDouble(0))
    //b1.update(1, b1.getLong(1) + b2.getLong(1))
    //b1
  //}
  
  //def finish(reduction: InternalRow): java.lang.Double = {
    //val count = reduction.getLong(1)
    //if (count == 0L) null
    //else reduction.getDouble(0) / count
  //}
  
  //def bufferEncoder: Encoder[InternalRow] = {
    //ExpressionEncoder(bufferSchema)
  //}
  
  //def outputEncoder: Encoder[java.lang.Double] = Encoders.DOUBLE
//}

// ############################# UNSAFE 2 #################################3
//import org.apache.spark.sql.expressions.Aggregator
//import org.apache.spark.sql.{Encoder, Encoders}
//import org.apache.spark.sql.catalyst.InternalRow
//import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
//import org.apache.spark.sql.catalyst.expressions.UnsafeRow
//import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
//import org.apache.spark.sql.types._

//class AggregateAvgScalaUnsafe extends Aggregator[Long, UnsafeRow, java.lang.Double] {
  
  //private val bufferSchema = StructType(Seq(
    //StructField("sum", DoubleType, nullable = false),
    //StructField("count", LongType, nullable = false)
  //))
  
  //private val projection = UnsafeProjection.create(bufferSchema)
  
  //def zero: UnsafeRow = {
    //val row = new UnsafeRow(2)
    //row.pointTo(new Array[Byte](16), 16)  // 8 bytes for double + 8 for long
    //row.setDouble(0, 0.0)
    //row.setLong(1, 0L)
    //row
  //}
  
  //def reduce(buffer: UnsafeRow, value: Long): UnsafeRow = {
    //buffer.setDouble(0, buffer.getDouble(0) + value.toDouble)
    //buffer.setLong(1, buffer.getLong(1) + 1L)
    //buffer
  //}
  
  //def merge(b1: UnsafeRow, b2: UnsafeRow): UnsafeRow = {
    //b1.setDouble(0, b1.getDouble(0) + b2.getDouble(0))
    //b1.setLong(1, b1.getLong(1) + b2.getLong(1))
    //b1
  //}
  
  //def finish(reduction: UnsafeRow): java.lang.Double = {
    //val count = reduction.getLong(1)
    //if (count == 0L) null
    //else reduction.getDouble(0) / count.toDouble
  //}
  
  //def bufferEncoder: Encoder[UnsafeRow] = {
    //ExpressionEncoder(bufferSchema).asInstanceOf[ExpressionEncoder[UnsafeRow]]
  //}
  
  //def outputEncoder: Encoder[java.lang.Double] = Encoders.DOUBLE
//}


// #########################3 MUTABLE CASE CLASS 2 ###################################
//import org.apache.spark.sql.expressions.Aggregator
//import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
//import org.apache.spark.sql.functions.udaf

//// Specialized for primitive types to avoid boxing
//@specialized
//case class AvgBuffer(var sum: Double, var count: Long) extends Serializable

//class AggregateAvgScala extends Aggregator[Long, AvgBuffer, java.lang.Double] {
  
  //def zero: AvgBuffer = AvgBuffer(0.0, 0L)
  
  //@inline
  //def reduce(buffer: AvgBuffer, value: Long): AvgBuffer = {
    //buffer.sum += value.toDouble
    //buffer.count += 1L
    //buffer
  //}
  
  //@inline
  //def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    //b1.sum += b2.sum
    //b1.count += b2.count
    //b1
  //}
  
  //def finish(reduction: AvgBuffer): java.lang.Double = {
    //if (reduction.count == 0L) null
    //else reduction.sum / reduction.count.toDouble
  //}
  
  //def bufferEncoder: Encoder[AvgBuffer] = Encoders.product[AvgBuffer]
  //def outputEncoder: Encoder[java.lang.Double] = Encoders.DOUBLE
//}

//object UDAFRegistration {
  //def registerUDAFs(spark: SparkSession): Unit = {
    //spark.udf.register("AggregateAvgScala", udaf(new AggregateAvgScala()))
  //}
//}

object UDAFRegistration {
  def registerUDAFs(spark: SparkSession): Unit = {
    spark.udf.register("AggregateAvgScala", udaf(new AggregateAvgScala()))
    //spark.udf.register("aggregate_median", udaf(new AggregateMedian()))
  }
}
