package preprocessing

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

trait Preprocessing
{
    type Processed = RDD[((String, String, String, String), Array[Double])]

    val time_batch: Int
    val storage_level: StorageLevel
    val partitions: Int

    def extract_features(file_uri : String): Processed
    def extract_streaming_features(batch: RDD[String]): RDD[(String, Array[Double])]
}

object Preprocessing
{
    def get_preprocessor(ss: SparkSession, kind: String): Preprocessing =
    {
        kind match {
            case "preprocess_core" =>
                new PreprocessingWithCore(ss.sparkContext,
                    time_batch = 10000,
                    storage_level = StorageLevel.MEMORY_ONLY,
                    partitions = 100)

            case "preprocess_sql" =>
                new PreprocessingWithSql(ss,
                    time_batch = 10000,
                    storage_level = StorageLevel.MEMORY_ONLY,
                    partitions = 100)

            case _ => throw new IllegalArgumentException("Invalid preprocessor type")
        }
    }
}