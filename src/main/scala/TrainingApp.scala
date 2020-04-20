import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import preprocessing.{Preprocessing, PreprocessingWithCore, PreprocessingWithSql}
import org.apache.spark.sql.SparkSession
import classification._

object TrainingApp 
{
    def main(args: Array[String])
    {
        val conf = new SparkConf()
        val ss = SparkSession.builder().config(conf).getOrCreate()

        val acc_file = args(0)
        val gyr_file = args(1)
        val classifier_params_uri = args(2)
        val rev_label_uri = args(3)

        val preprocessor: Preprocessing =
            new PreprocessingWithCore(ss.sparkContext, time_batch = 10000, StorageLevel.MEMORY_ONLY)
        //val preprocessor: Preprocessing =
        //    new PreprocessingWithSql(ss, time_batch = 10000, StorageLevel.MEMORY_ONLY)

        val acc_features = preprocessor.extract_features(acc_file)
        val gyr_features = preprocessor.extract_features(gyr_file)

        println(s"found ${acc_features.count()} accelerometer features " +
            s"and ${gyr_features.count()} gyroscope features ")

        val joined = acc_features.join(gyr_features)

        val struct = StructType(StructField("string_label", StringType) :: StructField("features", VectorType) :: Nil)
        val data = ss.createDataFrame(joined.map(it => Row(it._1._4, Vectors.dense(it._2._1 ++ it._2._2))),  struct)

        //val classifier = new DTClassifier(1234L)
        val classifier = new MLPClassifier(1234L, Array(18,40,6))
        classifier.train(data, classifier_params_uri, rev_label_uri)
    }
}