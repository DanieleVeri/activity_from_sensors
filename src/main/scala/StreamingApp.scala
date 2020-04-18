import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.functions.col
import scala.collection.JavaConversions.seqAsJavaList


object StreamingApp
{   // TODO: spark loglevel WARN
    def main(args: Array[String]) 
    {
        val classifier_params = "file:///home/dan/activity_from_sensors/params"

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

        val stream = ssc.socketTextStream("localhost", 7777)

        val acc_stream = stream.filter(s => s.endsWith("ACC"))
        val gyr_stream = stream.filter(s => s.endsWith("GYR"))

        val acc_windows = acc_stream.window(Seconds(10), Seconds(7))
        val gyr_windows = gyr_stream.window(Seconds(10), Seconds(7))

        val acc_features = acc_windows.transform(batch => extract_features(batch))
        val gyr_features = gyr_windows.transform(batch => extract_features(batch))

        val classifier = ssc.sparkContext.broadcast(MultilayerPerceptronClassificationModel.load(classifier_params))
        val struct = StructType(StructField("user", StringType) :: StructField("features", VectorType) :: Nil)

        val predicted_stream = acc_features.join(gyr_features).transform(batch => {
            val data = ss.createDataFrame(
                batch.map(it => Row(it._1, Vectors.dense(it._2._1 ++ it._2._2))),
                struct)

            val result = classifier.value.transform(data)
            result.select("user", "prediction").rdd
        })

        predicted_stream.print()

        ssc.start()
        ssc.awaitTermination()
    }

    // TODO: move in preprocessing
    def extract_features(batch: RDD[String]): RDD[(String, Array[Double])] = {
        val acc_array = batch.map(row => row.split(','))

        val hack = acc_array.map(arr => {
            arr(6) = arr(6).concat(s"_${arr(9)}")
            arr
        })

        // group by: user
        val acc_grouped = hack.groupBy(fields => fields(6))
        acc_grouped.persist(StorageLevel.MEMORY_ONLY)

        val acc_mean_xyz = acc_grouped.mapValues(sample_list => {
            val sum_count = sample_list.map(arr => (arr(3).toDouble, arr(4).toDouble, arr(5).toDouble, 1)).
                reduce((acm, it) => (acm._1 + it._1, acm._2 + it._2, acm._3 + it._3, acm._4 + 1))
            (sum_count._1 / sum_count._4, sum_count._2 / sum_count._4, sum_count._3 / sum_count._4)
        })

        val acc_var_xyz = acc_grouped.join(acc_mean_xyz).mapValues(group => {
            val sum_count = group._1.
                map(arr => (arr(3).toDouble, arr(4).toDouble, arr(5).toDouble, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0)).
                reduce((acm, it) => (
                    acm._1 + Math.pow(it._1 - group._2._1, 2),              // var x
                    acm._2 + Math.pow(it._2 - group._2._2, 2),              // var y
                    acm._3 + Math.pow(it._3 - group._2._3, 2),              // var z
                    acm._4 + (it._1 - group._2._1) * (it._2 - group._2._2), // cov xy
                    acm._5 + (it._1 - group._2._1) * (it._3 - group._2._3), // cov xz
                    acm._6 + (it._2 - group._2._2) * (it._3 - group._2._3), // cov yz
                    group._2._1,                                            // mean x
                    group._2._2,                                            // mean y
                    group._2._3,                                            // mean z
                    acm._10 + 1))
            Array(sum_count._1 / sum_count._10, sum_count._2 / sum_count._10, sum_count._3 / sum_count._10,
                sum_count._4 / sum_count._10, sum_count._5 / sum_count._10, sum_count._6 / sum_count._10,
                sum_count._7, sum_count._8, sum_count._9)
        })

        acc_var_xyz
    }
}