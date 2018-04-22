package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  // YOU NEED TO CHANGE THIS PART
  def calculateGScore(x: Int, y: Int, z: Int, S: Double, xBar: Double, numCells: Double, countMap: Map[String, Int]): Double = {
    val xList = List(x-1, x, x+1)
    val yList = List(y-1, y, y+1)
    val zList = List(z-1, z, z+1)

    var sigmaW = 0
    var sigmaWX = 0
    var sigmaW2 = 0
    for (i <- xList) {
      for (j <- yList) {
        for (k <- zList) {
          if (checkIfValidCell(i, j, k)) {
            sigmaW = sigmaW + 1
            sigmaWX = sigmaWX + getCellValue(i, j, k, countMap)
            sigmaW2 = sigmaW2 + (1 * 1)
          }
        }
      }
    }

    val numerator = sigmaWX - (xBar * sigmaW)
    val dUp = (numCells * sigmaW2) - math.pow(sigmaW, 2)
    val dDown = numCells - 1
    val denominator = S * math.sqrt(dUp/ dDown)
    val result = numerator / denominator

    return result
  }

  def getCellValue(x: Int, y: Int, z: Int, countMap: Map[String, Int]): Int = {
    val key = String.valueOf(x) + "|" + String.valueOf(y) + "|" + String.valueOf(z)
    if (countMap.contains(key)) {
      return countMap(key)
    } else {
      return 0
    }
  }

  def checkIfValidCell(x: Int, y: Int, z: Int): Boolean = {
    if (x >= -7450 && x <= -7370) {
      if (y >= 4050 && y <= 4090) {
        if (z >= 1 && z <= 31) {
          return true
        }
      }
    }

    return false
  }

}
