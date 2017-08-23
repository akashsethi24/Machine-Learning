package classification.naiveBayes

import classification.naiveBayes.GlobalData._

/**
  * Created by akash on 29/6/16.
  */
class Wind {

  def getWeakStatus: (Long, Long) = {

    val status = getSportRDD.filter(line => line.split(",")(5).toLowerCase.contains("weak"))
      .map { line => line.split(",")(1)
      }
    val yesCount = status.filter { word => word.toLowerCase.contains("yes") }.count()
    (yesCount, status.count() - yesCount)
  }

  def getStrongStatus: (Long, Long) = {

    val status = getSportRDD.filter(line => line.split(",")(5).toLowerCase.contains("strong"))
      .map { line => line.split(",")(1)
      }
    val yesCount = status.filter { word => word.toLowerCase.contains("yes") }.count()
    (yesCount, status.count() - yesCount)
  }
}
