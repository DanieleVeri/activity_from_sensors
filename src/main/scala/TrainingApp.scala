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
        val parsed_args = new ArgsParser(args)
        println(parsed_args)

        // Context build
        val conf = new SparkConf()
        val ss = SparkSession.builder().config(conf).getOrCreate()

        val run_mode = conf.get("spark.submit.deployMode")
        println("training in " + run_mode)

        val preprocessor = Preprocessing.get_preprocessor(ss, parsed_args.preprocess_type, parsed_args.partitions)

        val acc_features = preprocessor.extract_features(parsed_args.acc_uri)
        val gyr_features = preprocessor.extract_features(parsed_args.gyr_uri)
        val joined = acc_features.join(gyr_features)

        println(s"found ${joined.count()} samples")

        val struct = StructType(StructField("string_label", StringType) ::
            StructField("base_features", VectorType) :: Nil)
        val data = ss.createDataFrame(joined.map(it => Row(it._1._4, Vectors.dense(it._2._1 ++ it._2._2))),  struct)

        val classifier = Classifier.get_classifier(parsed_args.classifier_type)
        classifier.train(data, parsed_args.model_uri, parsed_args.label_uri)

        if (run_mode == "client") scala.io.StdIn.readLine()
    }

    class ArgsParser(args: Array[String]) {
        val acc_uri: String = args(0)
        val gyr_uri: String = args(1)
        val model_uri: String = args(2)
        val label_uri: String = args(3)
        val preprocess_type: String = args(4)
        val classifier_type: String = args(5)
        val partitions: Int = args(6).toInt

        override def toString: String = s"Configuration:\n" +
            s"  acc file URI: $acc_uri\n" +
            s"  gyr file URI: $gyr_uri\n" +
            s"  model file URI: $model_uri\n" +
            s"  label file URI: $label_uri\n" +
            s"  preprocess type: $preprocess_type\n" +
            s"  classifier_type: $classifier_type\n" +
            s"  partitions: $partitions"
    }

}
