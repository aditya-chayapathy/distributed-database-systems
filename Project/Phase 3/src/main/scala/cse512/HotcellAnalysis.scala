package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object HotcellAnalysis
{
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame = {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter", ";").option("header", "false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    //pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX", (pickupPoint: String) => ((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY", (pickupPoint: String) => ((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ", (pickupTime: String) => ((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5) as x,CalculateY(nyctaxitrips._c5) as y, CalculateZ(nyctaxitrips._c1) as z from nyctaxitrips")
    pickupInfo.createOrReplaceTempView("trimmedTaxiData")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName: _*)

    // Define the min and max of x, y, z
    val minX = -74.50 / HotcellUtils.coordinateStep
    val maxX = -73.70 / HotcellUtils.coordinateStep
    val minY = 40.50 / HotcellUtils.coordinateStep
    val maxY = 40.90 / HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

    // YOU NEED TO CHANGE THIS PART
    var spaceTimeCube = spark.sql("select x as cellX, y as cellY, z as cellZ, count(*) as cellCount from trimmedTaxiData group by x, y, z").persist()
    spaceTimeCube.createOrReplaceTempView("spaceTimeCube")
    var xList = spaceTimeCube.select("cellX").rdd.map(r => r(0)).collect()
    var yList = spaceTimeCube.select("cellY").rdd.map(r => r(0)).collect()
    var zList = spaceTimeCube.select("cellZ").rdd.map(r => r(0)).collect()
    var valueList = spaceTimeCube.select("cellCount").rdd.map(r => r(0)).collect()
    var countMap:Map[String, Int] = Map()
    for (i <- 0 to xList.length - 1) {
      val key = xList(i).toString + "|" + yList(i).toString + "|" + zList(i).toString
      val value = valueList(i).toString.toInt
      countMap += key -> value
    }

    spark.udf.register("square", (value: Double) => (value * value))
    var sumDF = spark.sql("select sum(cellCount) as sumCellCount, sum(square(cellCount)) as sumOfSquareCellCount from spaceTimeCube").persist()
    var sum = sumDF.select("sumCellCount").rdd.map(r => r(0)).collect()(0).toString.toInt
    var sumOfSquares = sumDF.select("sumOfSquareCellCount").rdd.map(r => r(0)).collect()(0).toString.toDouble
    var xBar = sum / numCells
    var leftPart = sumOfSquares / numCells
    var rightPart = math.pow(xBar, 2)
    var S = math.sqrt(leftPart - rightPart)

    spark.udf.register("calculateGScore", (x: Int, y: Int, z: Int) => (HotcellUtils.calculateGScore(x, y, z, S, xBar, numCells, countMap)))
    var finalResult = spark.sql("select cellX, cellY, cellZ from spaceTimeCube order by calculateGScore(cellX, cellY, cellZ) desc limit 50").persist()
    finalResult.show()

    return finalResult
  }
}
