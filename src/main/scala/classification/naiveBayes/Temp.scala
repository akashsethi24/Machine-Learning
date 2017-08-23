package classification.naiveBayes

import classification.naiveBayes.GlobalData._

/**
  * Created by akash on 29/6/16.
  */
class Temp {

  def getHotStatus: (Long, Long) = {

    val status = getSportRDD.filter(line => line.split(",")(3).toLowerCase.contains("hot"))
      .map { line => line.split(",")(1)
      }
    val yesCount = status.filter { word => word.toLowerCase.contains("yes") }.count()
    (yesCount, status.count() - yesCount)
  }

  def getMildStatus: (Long, Long) = {

    val status = getSportRDD.filter(line => line.split(",")(3).toLowerCase.contains("mild"))
      .map { line => line.split(",")(1)
      }
    val yesCount = status.filter { word => word.toLowerCase.contains("yes") }.count()
    (yesCount, status.count() - yesCount)
  }

  def getCoolStatus: (Long, Long) = {

    val status = getSportRDD.filter(line => line.split(",")(3).toLowerCase.contains("cool"))
      .map { line => line.split(",")(1)
      }
    val yesCount = status.filter { word => word.toLowerCase.contains("yes") }.count()
    (yesCount, status.count() - yesCount)
  }
}
