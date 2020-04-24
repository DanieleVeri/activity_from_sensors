package preprocessing

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

trait Preprocessing
{
    type Processed[KeyType] = RDD[(KeyType, Array[Double])]

    val time_batch: Int
    val storage_level: StorageLevel
    val partitions: Int

    def extract_features(file_uri : String): Processed[(String, String, String, String)]
    def extract_streaming_features(batch: RDD[String]): Processed[String]
}

object Preprocessing
{
    def get_preprocessor(ss: SparkSession, kind: String, partitions: Int): Preprocessing =
    {
        kind match {
            case "core" =>
                new PreprocessingWithCore(ss.sparkContext,
                    time_batch = 10000,
                    storage_level = StorageLevel.MEMORY_AND_DISK,
                    partitions)

            case "sql" =>
                new PreprocessingWithSql(ss,
                    time_batch = 10000,
                    storage_level = StorageLevel.MEMORY_AND_DISK,
                    partitions)

            case _ => throw new IllegalArgumentException("Invalid preprocessor type")
        }
    }
}