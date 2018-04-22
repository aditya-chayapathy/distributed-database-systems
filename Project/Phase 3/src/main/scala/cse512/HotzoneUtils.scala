package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
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

  // YOU NEED TO CHANGE THIS PART

}
