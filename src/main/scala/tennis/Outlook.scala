package tennis

/**
  * Created by akash on 29/6/16.
  */
class Outlook extends GlobalVariables {

  def getSunnyStatus: (Long, Long) = {

    val status = getSportRDD.filter(line => line.split(",")(1).equalsIgnoreCase("sunny"))
      .map { line => line.split(",")(5)
      }
    val yesCount = status.map { word => word.equalsIgnoreCase("yes") }.count()
    (yesCount, status.count() - yesCount)
  }

  def getOvercastStatus: (Long, Long) = {

    val status = getSportRDD.filter(line => line.split(",")(1).equalsIgnoreCase("overcast"))
      .map { line => line.split(",")(5)
      }
    val yesCount = status.map { word => word.equalsIgnoreCase("yes") }.count()
    (yesCount, status.count() - yesCount)
  }

  def getRainStatus: (Long, Long) = {

    val status = getSportRDD.filter(line => line.split(",")(1).equalsIgnoreCase("rain"))
      .map { line => line.split(",")(5)
      }
    val yesCount = status.map { word => word.equalsIgnoreCase("yes") }.count()
    (yesCount, status.count() - yesCount)
  }
}
