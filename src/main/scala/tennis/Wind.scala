package tennis

/**
  * Created by akash on 29/6/16.
  */
class Wind extends GlobalVariables{

  def getWeakStatus: (Long, Long) = {

    val status = getSportRDD.filter(line => line.split(",")(4).equalsIgnoreCase("weak"))
      .map { line => line.split(",")(5)
      }
    val yesCount = status.map { word => word.equalsIgnoreCase("yes") }.count()
    (yesCount, status.count() - yesCount)
  }

  def getStrongStatus: (Long, Long) = {

    val status = getSportRDD.filter(line => line.split(",")(4).equalsIgnoreCase("strong"))
      .map { line => line.split(",")(5)
      }
    val yesCount = status.map { word => word.equalsIgnoreCase("yes") }.count()
    (yesCount, status.count() - yesCount)
  }
}
