package cse512

import org.apache.spark.sql.SparkSession
import scala.math.pow

object SpatialQuery extends App{
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(contains(pointString, queryRectangle)))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(contains(pointString, queryRectangle)))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(within(pointString1, pointString2, distance)))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(within(pointString1, pointString2, distance)))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def contains(pointString:String, queryRectangle:String) : Boolean = {
    val x_pointString = pointString.split(",")(0).trim().toDouble
    val y_pointString = pointString.split(",")(1).trim().toDouble
    val p1_x_queryRectangle = queryRectangle.split(",")(0).trim().toDouble
    val p1_y_queryRectangle = queryRectangle.split(",")(1).trim().toDouble
    val p2_x_queryRectangle = queryRectangle.split(",")(2).trim().toDouble
    val p2_y_queryRectangle = queryRectangle.split(",")(3).trim().toDouble

    val min_x = Math.min(p1_x_queryRectangle, p2_x_queryRectangle)
    val max_x = Math.max(p1_x_queryRectangle, p2_x_queryRectangle)
    val min_y = Math.min(p1_y_queryRectangle, p2_y_queryRectangle)
    val max_y = Math.max(p1_y_queryRectangle, p2_y_queryRectangle)

    if(x_pointString >= min_x && x_pointString <= max_x && y_pointString >= min_y && y_pointString <= max_y) {
      true
    } else {
      false
    }
  }

  def within(pointString1:String, pointString2:String, distance:Double) : Boolean = {
    val x_pointString1 = pointString1.split(",")(0).trim().toDouble
    val y_pointString1 = pointString1.split(",")(1).trim().toDouble
    val x_pointString2 = pointString2.split(",")(0).trim().toDouble
    val y_pointString2 = pointString2.split(",")(1).trim().toDouble

    val actual_distance = pow(pow(x_pointString1 - x_pointString2, 2) + pow(y_pointString1 - y_pointString2, 2), 0.5)
    if(actual_distance <= distance) {
      true
    } else {
      false
    }
  }

}
