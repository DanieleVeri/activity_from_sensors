import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import preprocessing.{Preprocessing, PreprocessingWithCore, PreprocessingWithSql}
import classification.{DTModel, MLPModel, Model}
//import scala.collection.JavaConversions.seqAsJavaList


object StreamingApp
{   // TODO: spark loglevel WARN
    def main(args: Array[String]) 
    {
        val host = args(0)
        val port = args(1).toInt
        val classifier_params = args(2)
        val rev_label_params = args(3)

        val conf = new SparkConf()
        val ss = SparkSession.builder().config(conf).getOrCreate()
        val ssc = new StreamingContext(ss.sparkContext, Seconds(1))

        /* with checkpoint recovery
        val checkpointDir = "file:///home/dan/activity_from_sensors/checkpoints"
        def createStreamingContext() = {
            val sc = new SparkContext(conf)
            val ssc = new StreamingContext(sc, Seconds(1))
            ssc.checkpoint(checkpointDir)
            ssc
        }
        val ssc = StreamingContext.getOrCreate(checkpointDir, createStreamingContext)
        */

        val stream = ssc.socketTextStream(host, port)

        val preprocessor: Preprocessing = new PreprocessingWithSql(ss, time_batch = 10000, StorageLevel.MEMORY_ONLY)

        val acc_stream = stream.filter(s => s.endsWith("ACC"))
        val gyr_stream = stream.filter(s => s.endsWith("GYR"))

        val acc_windows = acc_stream.window(Seconds(10), Seconds(7))
        val gyr_windows = gyr_stream.window(Seconds(10), Seconds(7))

        val acc_features = acc_windows.transform(batch => preprocessor.extract_streaming_features(batch))
        val gyr_features = gyr_windows.transform(batch => preprocessor.extract_streaming_features(batch))

        //val model = new DTModel(classifier_params, rev_label_params)
        val model = new MLPModel(classifier_params, rev_label_params)
        val classifier = ssc.sparkContext.broadcast(model)

        val struct = StructType(StructField("user", StringType) :: StructField("features", VectorType) :: Nil)

        val predicted_stream = acc_features.join(gyr_features).transform(batch => {
            val data = ss.createDataFrame(
                batch.map(it => Row(it._1, Vectors.dense(it._2._1 ++ it._2._2))),
                struct)

            val result = classifier.value.transform(data)
            result.select("user", "predictedLabel").rdd
        })

        predicted_stream.print()

        ssc.start()
        ssc.awaitTermination()
    }

}