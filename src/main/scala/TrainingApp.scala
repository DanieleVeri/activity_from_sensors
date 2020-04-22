import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import preprocessing.Preprocessing
import org.apache.spark.sql.SparkSession
import classification.Classifier

object TrainingApp 
{
    def main(args: Array[String])
    {

        // Parse program args
        val acc_file = args(0)
        val gyr_file = args(1)
        val classifier_params_uri = args(2)
        val label_uri = args(3)
        val preprocess_kind = args(4)
        val classifier_kind = args(5)
        val partitions = args(6).toInt

        args.foreach(println)

        // Context build
        val conf = new SparkConf()
        val ss = SparkSession.builder().config(conf).getOrCreate()

        val run_mode = conf.get("spark.submit.deployMode")
        println("training " + run_mode)

        val preprocessor = Preprocessing.get_preprocessor(ss, preprocess_kind, partitions)

        val acc_features = preprocessor.extract_features(acc_file)
        val gyr_features = preprocessor.extract_features(gyr_file)

        //println(s"found ${acc_features.count()} accelerometer features " +
        //    s"and ${gyr_features.count()} gyroscope features ")

        val joined = acc_features.join(gyr_features)

        val struct = StructType(StructField("string_label", StringType) :: StructField("features", VectorType) :: Nil)
        val data = ss.createDataFrame(joined.map(it => Row(it._1._4, Vectors.dense(it._2._1 ++ it._2._2))),  struct)

        val classifier = Classifier.get_classifier(classifier_kind)
        classifier.train(data, classifier_params_uri, label_uri)

        if (run_mode == "client")
            scala.io.StdIn.readLine()
    }

}