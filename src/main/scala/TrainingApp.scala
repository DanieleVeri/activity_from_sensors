import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

//import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object TrainingApp 
{
    def main(args: Array[String])
    {
        val conf = new SparkConf()
        val ss = SparkSession.builder().config(conf).getOrCreate()

        // TODO: take from program args
        val acc_file = "file:///home/dan/activity_from_sensors/data/acc00.csv"
        val gyr_file = "file:///home/dan/activity_from_sensors/data/gyr00.csv"

        //val acc_stats = Preprocessing.with_spark_core(ss.sparkContext, acc_file)
        val acc_stats = Preprocessing.with_spark_sql(ss, acc_file)
        println(acc_stats.count())

        val struct = StructType(
            StructField("label", StringType) ::
            StructField("features", VectorType) :: Nil)

        val data = ss.createDataFrame(acc_stats.map(it => Row(it._1._4,
            Vectors.dense(it._2._1, it._2._2, it._2._3, it._2._4, it._2._5, it._2._6))), struct)

        data.show(2)

        val labelIndexer = new StringIndexer()
            .setInputCol("label")
            .setOutputCol("indexedLabel")
            .fit(data)
        // Automatically identify categorical features, and index them.
        val featureIndexer = new VectorIndexer()
            .setInputCol("features")
            .setOutputCol("indexedFeatures")
            .setMaxCategories(4) // features with > 4 distinct values are treated as continuous.
            .fit(data)

        // Split the data into training and test sets (30% held out for testing).
        val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

        // Train a DecisionTree model.
        val dt = new DecisionTreeClassifier()
            .setLabelCol("indexedLabel")
            .setFeaturesCol("indexedFeatures")

        // Convert indexed labels back to original labels.
        val labelConverter = new IndexToString()
            .setInputCol("prediction")
            .setOutputCol("predictedLabel")
            .setLabels(labelIndexer.labels)

        // Chain indexers and tree in a Pipeline.
        val pipeline = new Pipeline()
            .setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))

        // Train model. This also runs the indexers.
        val model = pipeline.fit(trainingData)

        // Make predictions.
        val predictions = model.transform(testData)

        // Select example rows to display.
        predictions.select("predictedLabel", "label", "features").show(5)

        // Select (prediction, true label) and compute test error.
        val evaluator = new MulticlassClassificationEvaluator()
            .setLabelCol("indexedLabel")
            .setPredictionCol("prediction")
            .setMetricName("accuracy")
        val accuracy = evaluator.evaluate(predictions)
        println(s"Test Error = ${(1.0 - accuracy)}")

        val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
        println(s"Learned classification tree model:\n ${treeModel.toDebugString}")

        scala.io.StdIn.readLine()
    }
}