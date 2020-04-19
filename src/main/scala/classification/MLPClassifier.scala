package classification

import org.apache.spark.ml.classification.{MultilayerPerceptronClassificationModel, MultilayerPerceptronClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.DataFrame

class MLPClassifier extends Classifier[MultilayerPerceptronClassificationModel]
{
  override def train(data: DataFrame, layers: Array[Int]: MultilayerPerceptronClassificationModel =
    {
      val indexer = new StringIndexer()
        .setInputCol("label")
        .setOutputCol("indexedLabel")
        .fit(data)

      val indexed = indexer.transform(data)
      println("data")
      data.show(20)
      println("indexed")
      indexed.show(20)

      // Split the data into train and test
      val splits = indexed.randomSplit(Array(0.7, 0.3))
      val train = splits(0)
      val test = splits(1)

      // create the trainer and set its parameters
      val trainer = new MultilayerPerceptronClassifier()
        .setLayers(layers)
        .setLabelCol("indexedLabel")
        .setBlockSize(128)
        .setMaxIter(100)

      // train the model
      val model = trainer.fit(train)

      // compute accuracy on the test set
      val result = model.transform(test)
      val predictionAndLabels = result.select("prediction", "label")
      val evaluator = new MulticlassClassificationEvaluator()
        .setMetricName("accuracy")

      println(s"Test set accuracy = ${evaluator.evaluate(predictionAndLabels)}")

      model
    }

  override def load_model(params_uri: String): MultilayerPerceptronClassificationModel =
    {
      MultilayerPerceptronClassificationModel.load(params_uri)
    }
}
