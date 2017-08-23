package classification.naiveBayes

import java.io.PrintWriter

/**
  * Created by akash on 29/6/16.
  */
object FileProcessing extends App {

  val directory = System.getProperty("user.dir") + "/src/main/resources"
  val out = new PrintWriter(directory + "/Data.txt", "UTF-8")

  def cleanFile: Boolean = {

    val cleanData = GlobalData.getSportRDD.map { line =>
      val fields = line.split(",")
      fields.toList.map { word => if (replaceConditionforOne(word)) "1"
      else if (replaceConditionforTwo(word)) "2"
      else if (word.replace("\"\"", "").toLowerCase.contains("yes")) "0"
      else "3"
      }
    }
    println(GlobalData.getSportRDD.collect().toList)
    println(cleanData.collect().toList.map { list => getStringFromList(list) })
    out.close()
    true
  }

  def replaceConditionforOne(word: String): Boolean = {

    word.toLowerCase.replace("\"", "") match {
      case "sunny" | "hot" | "strong" | "no" | "high" => true
      case _ => false
    }
  }

  def replaceConditionforTwo(word: String): Boolean = {

    word.toLowerCase.replace("\"", "") match {
      case "overcast" | "mild" | "normal" | "weak" => true
      case _ => false
    }
  }

  def getStringFromList(list: List[String]): Boolean = {

    try {
      list.foreach { word => out.write(word + "\t")
      }
      out.write("\n")
      true
    } catch {
      case ex: Exception => false
    }
  }

  cleanFile
}
