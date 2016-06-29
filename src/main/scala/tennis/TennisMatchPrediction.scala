package tennis

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

/**
  * Created by akash on 29/6/16.
  */
class TennisMatchPrediction {

  def trainData: Boolean = {

    val directory = System.getProperty("user.dir") + "/src/main/resources"
    val trainingRDD = GlobalData.sc.textFile(directory + "/Data.txt")
    val trainingLabelPoint = trainingRDD.map { line =>
      val fields = line.split("\t")
      LabeledPoint.apply(fields(4).toDouble, Vectors.dense(Array(fields(0).toDouble, fields(1).toDouble, fields(2).toDouble, fields(3).toDouble)))
    }

    val splits = trainingLabelPoint.randomSplit(Array(0.4, 0.6), seed = 11L)
    val trainingData = splits(0)
    val testData = splits(1)

    val model = NaiveBayes.train(trainingData, lambda = 1.0, modelType = "multinomial")
    val predictAndLabel = testData.map { test => (model.predict(test.features), test.label) }
    val accurancy = 1.0 * predictAndLabel.filter(data => data._1 == data._2).count() / testData.count()

    model.save(GlobalData.sc, directory + "/Predicted")

    true
  }

}

object TennisMatchPrediction extends App {

  val obj = new TennisMatchPrediction
  println(obj.trainData)
  GlobalData.sc.stop()
}
