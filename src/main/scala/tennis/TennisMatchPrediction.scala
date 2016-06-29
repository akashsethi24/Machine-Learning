package tennis

import java.io.{BufferedWriter, FileWriter, PrintWriter}
import java.util.Scanner

import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

/**
  * Created by akash on 29/6/16.
  */
class TennisMatchPrediction extends App {


}

object TennisMatchPrediction extends App {

  GlobalData.sc.stop()
}
