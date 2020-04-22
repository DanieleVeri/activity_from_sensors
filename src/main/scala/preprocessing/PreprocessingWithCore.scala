package preprocessing

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

class PreprocessingWithCore(val sc: SparkContext,
                            override val time_batch:Int,
                            override val storage_level: StorageLevel,
                            override val partitions: Int) extends Preprocessing with Serializable
{
    override def extract_features(file_uri: String): Processed =
    {
        val string = sc.textFile(file_uri)
        val array = string.map(row => row.split(','))
        val array_notnull = array.filter(arr => arr(9) != "null" && arr(0) != "Index")

        // avoid class serialization
        val local_time_batch = time_batch
        val array_time_batched = array_notnull.map(arr => {
            val time = arr(1).toLong / local_time_batch
            arr(1) = time.toString
            arr
        })

        // group by: time, user, device, activity
        val features = compute_variance[(String, String, String, String)](array_time_batched,
            fields => (fields(1), fields(6), fields(8), fields(9)))

        features.persist(storage_level)
    }

    def extract_streaming_features(batch: RDD[String]): RDD[(String, Array[Double])] =
    {
        val array = batch.map(row => row.split(','))
        val name_label = array.map(arr => {
            arr(6) = arr(6).concat(s"(${arr(9)})")
            arr
        })

        // group by: user
        val features = compute_variance[String](name_label, fields => fields(6))

        features.persist(storage_level)
    }

    def compute_variance[K: ClassTag](collection: RDD[Array[String]], group_lambda: Array[String] => K): RDD[(K, Array[Double])] =
    {
        val grouped = collection.groupBy(group_lambda, new HashPartitioner(partitions))
        grouped.persist(storage_level)

        val mean_xyz = grouped.mapValues(sample_list => {
            val sum_count = sample_list.
                map(arr => (arr(3).toDouble, arr(4).toDouble, arr(5).toDouble, 1)).
                reduce((acm, it) => (acm._1 + it._1, acm._2 + it._2, acm._3 + it._3, acm._4 + 1))
            (sum_count._1 / sum_count._4, sum_count._2 / sum_count._4, sum_count._3 / sum_count._4)
        })

        val features = grouped.join(mean_xyz).mapValues(group => {
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

        features
    }
}
