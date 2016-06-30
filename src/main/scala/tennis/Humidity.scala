package tennis

import GlobalData._

/**
  * Created by akash on 29/6/16.
  */
class Humidity {

  def getHighStatus: (Long, Long) = {

    val status = getSportRDD.filter(line => line.split(",")(4).toLowerCase.contains("high"))
      .map { line => line.split(",")(1)
      }
    val yesCount = status.filter { word => word.toLowerCase.contains("yes") }.count()
    (yesCount, status.count() - yesCount)
  }

  def getNormalStatus: (Long, Long) = {

    val status = getSportRDD.filter(line => line.split(",")(4).toLowerCase.contains("normal"))
      .map { line => line.split(",")(1)
      }
    val yesCount = status.filter { word => word.toLowerCase.contains("yes") }.count()
    (yesCount, status.count() - yesCount)
  }
}
