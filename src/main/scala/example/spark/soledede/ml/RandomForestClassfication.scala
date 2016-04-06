package example.spark.soledede.ml

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, VectorAssembler, StringIndexer}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by soledede on 16/4/6.
  */
object RandomForestClassfication {

  def main(args: Array[String]) {
    if (args.length < 1) {
      println("Usage:RandomForest ClassificationPipeline inputDataFile")
      sys.exit(1)
    }
    val conf = new SparkConf().setAppName("Classification with ML Pipeline")
    val sc = new SparkContext(conf)
    val sqlCtx = new SQLContext(sc)

    /** Step 1
      * Read the source data file and convert it to be a dataframe with columns named.
      * 3.6216,8.6661,-2.8073,-0.44699,0
      * 4.5459,8.1674,-2.4586,-1.4621,0
      * 3.866,-2.6383,1.9242,0.10645,0
      * 3.4566,9.5228,-4.0112,-3.5944,1
      * 0.32924,-4.4552,4.5718,-0.9888,0
      * 4.3684,9.6718,-3.9606,-3.1625,0
      * 3.5912,3.0129,0.72888,0.56421,1
      * 2.0922,-6.81,8.4636,-0.60216,0
      * 3.2032,5.7588,-0.75345,-0.61251,0

      * ... ...
      */
    val parsedRDD = sc.textFile(args(0)).map(_.split(",")).map(eachRow => {
      val a = eachRow.map(x => x.toDouble)
      (a(0), a(1), a(2), a(3), a(4))
    })
    val df = sqlCtx.createDataFrame(parsedRDD).toDF(
      "f0", "f1", "f2", "f3", "label").cache()

    /** *
      * Step 2
      * StringIndexer encodes a string column of labels
      * to a column of label indices. The indices are in [0, numLabels),
      * ordered by label frequencies.
      * This can help detect label in raw data and give it an index automatically.
      * So that it can be easily processed by existing spark machine learning algorithms.
      * */
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(df)

    /**
      * Step 3
      * Define a VectorAssembler transformer to transform source features data to be a vector
      * This is helpful when raw input data contains non-feature columns, and it is common for
      * such a input data file to contain columns such as "ID", "Date", etc.
      */
    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("f0", "f1", "f2", "f3"))
      .setOutputCol("featureVector")

    /**
      * Step 4
      * Create RandomForestClassifier instance and set the input parameters.
      * Here we will use 5 trees Random Forest to train on input data.
      */
    val rfClassifier = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("featureVector")
      .setNumTrees(5)

    /**
      * Step 5
      * Convert indexed class labels back to original one so that it can be easily understood when we
      * need to display or save the prediction result to a file.
      */
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    //Step 6
    //Randomly split the input data by 8:2, while 80% is for training, the rest is for testing.
    val Array(trainingData, testData) = df.randomSplit(Array(0.8, 0.2))

    /**
      * Step 7
      * Create a ML pipeline which is constructed by for 4 PipelineStage objects.
      * and then call fit method to perform defined operations on training data.
      */
    val pipeline = new Pipeline().setStages(Array(labelIndexer, vectorAssembler, rfClassifier, labelConverter))
    val model = pipeline.fit(trainingData)

    /**
      * Step 8
      * Perform predictions about testing data. This transform method will return a result DataFrame
      * with new prediction column appended towards previous DataFrame.
      *
      **/
    val predictionResultDF = model.transform(testData)

    /**
      * Step 9
      * Select features,label,and predicted label from the DataFrame to display.
      * We only show 20 rows, it is just for reference.
      */
    predictionResultDF.select("f0", "f1", "f2", "f3", "label", "predictedLabel").show(20)

    /**
      * Step 10
      * The evaluator code is used to compute the prediction accuracy, this is
      * usually a valuable feature to estimate prediction accuracy the trained model.
      */
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("precision")
    val predictionAccuracy = evaluator.evaluate(predictionResultDF)
    println("Testing Error = " + (1.0 - predictionAccuracy))
    /**
      * Step 11(Optional)
      * You can choose to print or save the the model structure.
      */
    val randomForestModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
    println("Trained Random Forest Model is:\n" + randomForestModel.toDebugString)
  }

}
