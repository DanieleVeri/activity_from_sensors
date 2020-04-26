package preprocessing

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import scala.reflect.ClassTag
import java.lang.Math._


class PreprocessingWithCore(val sc: SparkContext,
                            override val time_batch:Int,
                            override val storage_level: StorageLevel,
                            override val partitions: Int) extends Preprocessing with Serializable
{
    override def extract_features(file_uri: String): Processed[(String, String, String, String)] =
    {
        val lines = sc.textFile(file_uri)
        val arrays = lines.map(row => row.split(','))
        val array_notnull = arrays.filter(arr => arr(9) != "null" && arr(0) != "Index")

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

    def extract_streaming_features(batch: RDD[String]): Processed[String] =
    {
        val array = batch.map(row => row.split(','))
        val name_label = array.map(arr => {
            arr(6) = arr(6).concat(s" with ${arr(8)} does ${arr(9)}")
            arr
        })

        // group by: user and device
        val features = compute_variance[String](name_label, fields => fields(6))
        features.persist(storage_level)
    }

    def compute_variance[K: ClassTag](collection: RDD[Array[String]], group_lambda: Array[String] => K): Processed[K] =
    {
        val pairs = collection.map(arr => (group_lambda(arr),
                Array(arr(3).toDouble, arr(4).toDouble, arr(5).toDouble))).
            partitionBy(new HashPartitioner(partitions)).
            persist(storage_level)

        val mean_init = Array.ofDim[Double](4)
        val means = pairs.foldByKey(mean_init, partitions / 5)((acm, it) => {
            acm(0) += it(0)                                                                     // mean x
            acm(1) += it(1)                                                                     // mean y
            acm(2) += it(2)                                                                     // mean z
            acm(3) += 1
            acm
        }).mapValues(acm => {
            acm(0) /= acm(3)
            acm(1) /= acm(3)
            acm(2) /= acm(3)
            acm
        })

        val var_init = (Array.ofDim[Double](10), Array.emptyDoubleArray)
        val variances = pairs.join(means).foldByKey(var_init, partitions / 5)((acm, it) => {
            acm._1(0) += pow(it._1(0) - it._2(0), 2)                                            // var x
            acm._1(1) += pow(it._1(1) - it._2(1), 2)                                            // var y
            acm._1(2) += pow(it._1(2) - it._2(2), 2)                                            // var z
            acm._1(3) += (it._1(0) - it._2(0)) * (it._1(1) - it._2(1))                          // cov xy
            acm._1(4) += (it._1(0) - it._2(0)) * (it._1(2) - it._2(2))                          // cov xz
            acm._1(5) += (it._1(1) - it._2(1)) * (it._1(2) - it._2(2))                          // cov yz
            acm._1(6) = it._2(3)
            acm._1(7) = it._2(0)
            acm._1(8) = it._2(1)
            acm._1(9) = it._2(2)
            acm
        }).mapValues(acm => {
            acm._1(0) /= acm._1(6)
            acm._1(1) /= acm._1(6)
            acm._1(2) /= acm._1(6)
            acm._1(3) /= acm._1(6)
            acm._1(4) /= acm._1(6)
            acm._1(5) /= acm._1(6)
            acm._1
        })

        val std_moments_init = (Array.ofDim[Double](15), Array.emptyDoubleArray)
        val std_moments = pairs.join(variances).foldByKey(std_moments_init, partitions / 5)((acm, it) => {
            acm._1(0) += pow((it._1(0) - it._2(7)) / sqrt(it._2(0)), 3)                        // skewness x
            acm._1(1) += pow((it._1(1) - it._2(8)) / sqrt(it._2(1)), 3)                        // skewness y
            acm._1(2) += pow((it._1(2) - it._2(9)) / sqrt(it._2(2)), 3)                        // skewness z
            acm._1(3) += pow((it._1(0) - it._2(7)) / sqrt(it._2(0)), 4)                        // kurtosis x
            acm._1(4) += pow((it._1(1) - it._2(8)) / sqrt(it._2(1)), 4)                        // kurtosis y
            acm._1(5) += pow((it._1(2) - it._2(9)) / sqrt(it._2(2)), 4)                        // kurtosis z
            acm._1(6) = it._2(0)
            acm._1(7) = it._2(1)
            acm._1(8) = it._2(2)
            acm._1(9) = it._2(3)
            acm._1(10) = it._2(4)
            acm._1(11) = it._2(5)
            acm._1(12) = it._2(7)
            acm._1(13) = it._2(8)
            acm._1(14) = it._2(9)
            acm
        }).mapValues(acm => acm._1)

        std_moments
    }

}