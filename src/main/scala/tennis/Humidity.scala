package tennis

/**
  * Created by akash on 29/6/16.
  */
class Humidity extends GlobalVariables {

  def getHighStatus: (Long, Long) = {

    val status = getSportRDD.filter(line => line.split(",")(3).equalsIgnoreCase("high"))
      .map { line => line.split(",")(5)
      }
    val yesCount = status.map { word => word.equalsIgnoreCase("yes") }.count()
    (yesCount, status.count() - yesCount)
  }

  def getNormalStatus: (Long, Long) = {

    val status = getSportRDD.filter(line => line.split(",")(3).equalsIgnoreCase("normal"))
      .map { line => line.split(",")(5)
      }
    val yesCount = status.map { word => word.equalsIgnoreCase("yes") }.count()
    (yesCount, status.count() - yesCount)
  }
}
