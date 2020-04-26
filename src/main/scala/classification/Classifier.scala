package classification
import java.io.IOException

import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, MinMaxScaler, StringIndexer, StringIndexerModel}
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.DataFrame


abstract class Classifier(val seed: Long)
{
    val trainer: PipelineStage

    def train(data: DataFrame, params_uri: String, label_uri: String): Unit =
    {

        var found_labels = false
        var label_indexer: StringIndexerModel = null

        try {
            label_indexer = StringIndexerModel.load(label_uri)
            found_labels = true
        }
        catch
        {
            case _: IOException =>
            {
                println("labels not found, forcing StringIndexer computation")
                label_indexer = new StringIndexer()
                  .setInputCol("string_label")
                  .setOutputCol("label")
                  .fit(data)
            }
        }
        println(s"Found labels: ${label_indexer.labels.mkString("[", ", ", "]")}")

        val scaler = new MinMaxScaler().setInputCol("base_features").setOutputCol("features").fit(data)

        // Split the data into training and test sets (30% held out for testing).
        val Array(training, test) = data.randomSplit(Array(0.7, 0.3), seed)

        // Convert indexed labels back to original labels.
        val labelConverter = new IndexToString()
            .setInputCol("prediction")
            .setOutputCol("predictedLabel")
            .setLabels(label_indexer.labels)

        // Chain indexers and tree in a Pipeline.
        val pipeline = new Pipeline()
            .setStages(Array(label_indexer, scaler, trainer, labelConverter))


        // Train model. This also runs the indexers.
        val model = pipeline.fit(training)

        // Make predictions.
        val predictions = model.transform(test)

        // Select example rows to display.
        predictions.select("string_label", "predictedLabel", "features").show(20)

        // Select (prediction, true label) and compute test error.
        val evaluator = new MulticlassClassificationEvaluator()
            .setPredictionCol("prediction")
            .setMetricName("accuracy")
        val accuracy = evaluator.evaluate(predictions)
        println(s"Test accuracy = $accuracy")

        // Save all trainer params (and hyperparams)
        model.stages(2).asInstanceOf[MLWritable].write.overwrite().save(params_uri)
        model.stages(1).asInstanceOf[MLWritable].write.overwrite().save(params_uri + "_scaler")

        if (!found_labels)
        {
            model.stages(0).asInstanceOf[StringIndexerModel].save(label_uri)
            model.stages(3).asInstanceOf[IndexToString].save(label_uri + "_reverse")
        }
    }
}

object Classifier
{
    def get_classifier(kind: String): Classifier =
    {
        kind match {
            case "dt" =>
                new DTClassifier(1234L)
            case "mlp" =>
                new MLPClassifier(1234L, Array(30, 60, 6))
            case _ => throw new IllegalArgumentException("Invalid classifier")
        }
    }
}