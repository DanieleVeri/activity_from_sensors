package classification
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.DataFrame


abstract class Classifier(val seed: Long)
{
    val trainer: PipelineStage

    def train(data: DataFrame, params_uri: String, rev_label_uri: String): Unit =
    {
        val label_indexer = new StringIndexer()
            .setInputCol("string_label")
            .setOutputCol("label")
            .fit(data)
        println(s"Found labels: ${label_indexer.labels.mkString("[", ", ", "]")}")

        // Split the data into training and test sets (30% held out for testing).
        val Array(training, test) = data.randomSplit(Array(0.7, 0.3), seed)

        // Convert indexed labels back to original labels.
        val labelConverter = new IndexToString()
            .setInputCol("prediction")
            .setOutputCol("predictedLabel")
            .setLabels(label_indexer.labels)

        // Chain indexers and tree in a Pipeline.
        val pipeline = new Pipeline()
            .setStages(Array(label_indexer, trainer, labelConverter))

        // Train model. This also runs the indexers.
        val model = pipeline.fit(training)

        // Make predictions.
        val predictions = model.transform(test)

        // Select example rows to display.
        predictions.select("predictedLabel", "label", "features").show(20)

        // Select (prediction, true label) and compute test error.
        val evaluator = new MulticlassClassificationEvaluator()
            .setPredictionCol("prediction")
            .setMetricName("accuracy")
        val accuracy = evaluator.evaluate(predictions)
        println(s"Test accuracy = $accuracy")

        model.stages(1).asInstanceOf[MLWritable].save(params_uri)
        model.stages(2).asInstanceOf[IndexToString].save(rev_label_uri)
    }
}

object Classifier
{
    def get_classifier(kind: String): Classifier =
    {
        kind match {
            case "dt_classifier" =>
                new DTClassifier(1234L)
            case "mlp_classifier" =>
                new MLPClassifier(1234L, Array(18, 40, 6))
            case _ => throw new IllegalArgumentException("Invalid classifier")
        }
    }
}