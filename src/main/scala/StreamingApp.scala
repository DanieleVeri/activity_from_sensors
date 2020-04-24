import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import preprocessing.Preprocessing
import classification.Model


object StreamingApp
{
    def main(args: Array[String]) 
    {
        val parsed_args = new ArgsParser(args)
        println(parsed_args)

        // Context build
        val conf = new SparkConf()
        val ss = SparkSession.builder().config(conf).getOrCreate()
        val ssc = new StreamingContext(ss.sparkContext, Seconds(1))

        // with checkpoint recovery
        /*
        val checkpointDir = "file:///home/dan/activity_from_sensors/checkpoints"
        def createStreamingContext() = {
            val sc = new SparkContext(conf)
            val ssc = new StreamingContext(sc, Seconds(1))
            ssc.checkpoint(checkpointDir)
            ssc
        }
        val ssc = StreamingContext.getOrCreate(checkpointDir, createStreamingContext)
        */

        val stream = ssc.socketTextStream(parsed_args.server_host, parsed_args.server_port)

        val acc_stream = stream.filter(s => s.endsWith("ACC"))
        val gyr_stream = stream.filter(s => s.endsWith("GYR"))

        val acc_windows = acc_stream.window(Seconds(10), Seconds(7))
        val gyr_windows = gyr_stream.window(Seconds(10), Seconds(7))

        val preprocessor = Preprocessing.get_preprocessor(ss, parsed_args.preprocess_type, parsed_args.partitions)

        val acc_features = acc_windows.transform(batch => preprocessor.extract_streaming_features(batch))
        val gyr_features = gyr_windows.transform(batch => preprocessor.extract_streaming_features(batch))

        val model = Model.get_model(parsed_args.classifier_type, parsed_args.model_uri, parsed_args.label_uri)
        val broadcast_model = ssc.sparkContext.broadcast(model)

        val struct = StructType(StructField("user", StringType) :: StructField("features", VectorType) :: Nil)

        val predicted_stream = acc_features.join(gyr_features).transform(batch => {
            val data = ss.createDataFrame(batch.map(it => Row(it._1, Vectors.dense(it._2._1 ++ it._2._2))), struct)
            val result = broadcast_model.value.transform(data)
            result.select("user", "predictedLabel").rdd
        })

        predicted_stream.print()
        predicted_stream.saveAsTextFiles(parsed_args.out_uri, ss.sparkContext.applicationId)

        ssc.start()
        ssc.awaitTermination()
    }

    class ArgsParser(args: Array[String]) {
        val server_host: String = args(0)
        val server_port: Int = args(1).toInt
        val model_uri: String = args(2)
        val label_uri: String = args(3)
        val preprocess_type: String = args(4)
        val classifier_type: String = args(5)
        val out_uri: String = args(6)
        val partitions: Int = args(7).toInt

        override def toString: String = s"Configuration:\n " +
            s"server host: $server_host\n " +
            s"server port: $server_port\n " +
            s"model file URI: $model_uri\n " +
            s"label file URI: $label_uri\n " +
            s"preprocess type: $preprocess_type\n " +
            s"classifier_type: $classifier_type\n " +
            s"stream output URI: $out_uri\n " +
            s"partitions: $partitions"
    }
}