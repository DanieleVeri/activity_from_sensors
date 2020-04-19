package preprocessing

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

trait Preprocessing
{
    type Processed = RDD[((String, String, String, String), Array[Double])]

    val time_batch: Int
    val storage_level: StorageLevel

    def extract_features(file_uri : String): Processed
    def extract_streaming_features(batch: RDD[String]): RDD[(String, Array[Double])]
}
