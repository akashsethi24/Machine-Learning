package tennis

/**
  * Created by akash on 29/6/16.
  */
class Temp extends GlobalVariables{

  def getHotStatus: (Long, Long) = {

    val status = getSportRDD.filter(line => line.split(",")(2).equalsIgnoreCase("hot"))
      .map { line => line.split(",")(5)
      }
    val yesCount = status.map { word => word.equalsIgnoreCase("yes") }.count()
    (yesCount, status.count() - yesCount)
  }

  def getMildStatus: (Long, Long) = {

    val status = getSportRDD.filter(line => line.split(",")(2).equalsIgnoreCase("mild"))
      .map { line => line.split(",")(5)
      }
    val yesCount = status.map { word => word.equalsIgnoreCase("yes") }.count()
    (yesCount, status.count() - yesCount)
  }

  def getCoolStatus: (Long, Long) = {

    val status = getSportRDD.filter(line => line.split(",")(2).equalsIgnoreCase("cool"))
      .map { line => line.split(",")(5)
      }
    val yesCount = status.map { word => word.equalsIgnoreCase("yes") }.count()
    (yesCount, status.count() - yesCount)
  }
}
