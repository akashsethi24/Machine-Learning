package classification.naiveBayes

import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

/**
  * Powered by akash on 29/6/16.
  */
class TennisMatchPrediction {

  val directory = System.getProperty("user.dir") + "/src/main/resources"

  def trainData: NaiveBayesModel = {

    val trainingRDD = GlobalData.sc.textFile(directory + "/Data.txt")
    val trainingLabelPoint = trainingRDD.map { line =>
      val fields = line.split(",")
      LabeledPoint.apply(fields(4).toDouble, Vectors.dense(Array(fields(0).toDouble, fields(1).toDouble, fields(2).toDouble, fields(3).toDouble)))
    }

    val splits = trainingLabelPoint.randomSplit(Array(0.4, 0.6), seed = 11L)
    val trainingData = splits(0)
    val testData = splits(1)
    val model = NaiveBayes.train(trainingLabelPoint, 1.0)

    val predictAndLabel = testData.map { test => (model.predict(test.features), test.label) }
    val accuracy = 1.0 * predictAndLabel.filter(data => data._1 == data._2).count() / testData.count()
    println("Accuracy  := " + accuracy)

    model
  }

  def predictPlay(model: NaiveBayesModel): Boolean = {

    val inputRdd = GlobalData.sc.textFile(directory + "/Input.txt")
    val vector = inputRdd.map { line =>
      val fields = line.split(",")
      Vectors.dense(Array(fields(0).toDouble, fields(1).toDouble, fields(2).toDouble, fields(3).toDouble))
    }
    val output = model.predictProbabilities(vector)
    println("Prediction of Playing Game :- " + output.collect().toList)
    true
  }

}

object TennisMatchPrediction extends App {

  val obj = new TennisMatchPrediction
  obj.predictPlay(obj.trainData)
  GlobalData.sc.stop()
}
