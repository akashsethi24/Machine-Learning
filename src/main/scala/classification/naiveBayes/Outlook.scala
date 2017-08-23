package classification.naiveBayes

import classification.naiveBayes.GlobalData._

/**
  * Created by akash on 29/6/16.
  */
class Outlook {

  def getSunnyStatus: (Long, Long) = {

    val status = getSportRDD.filter(line => line.split(",")(2).toLowerCase.contains("sunny"))
      .map { line => line.split(",")(1)
      }
    val yesCount = status.filter { word => word.toLowerCase.contains("yes") }.count()
    (yesCount, status.count() - yesCount)
  }

  def getOvercastStatus: (Long, Long) = {

    val status = getSportRDD.filter(line => line.split(",")(2).toLowerCase.contains("overcast"))
      .map { line => line.split(",")(1)
      }
    val yesCount = status.filter { word => word.toLowerCase.contains("yes") }.count()
    (yesCount, status.count() - yesCount)
  }

  def getRainStatus: (Long, Long) = {

    val status = getSportRDD.filter(line => line.split(",")(2).toLowerCase.contains("rain"))
      .map { line => line.split(",")(1)
      }
    val yesCount = status.filter { word => word.toLowerCase.contains("yes") }.count()
    (yesCount, status.count() - yesCount)
  }
}
