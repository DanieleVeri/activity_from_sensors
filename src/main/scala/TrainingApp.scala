import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier, MultilayerPerceptronClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import preprocessing.{Preprocessing, PreprocessingWithSql}

//import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object TrainingApp 
{

  def main(args: Array[String])
    {
        val conf = new SparkConf()
        val ss = SparkSession.builder().config(conf).getOrCreate()

        val acc_file = args(0)
        val gyr_file = args(1)
        val classifier_params = args(2)

        //val acc_stats = preprocessing.Preprocessing.with_spark_core(ss.sparkContext, acc_file)

        val preprocessor: Preprocessing = new PreprocessingWithSql(ss, time_batch = 10000, StorageLevel.MEMORY_ONLY)
        val acc_features = preprocessor.extract_features(acc_file)
        val gyr_features = preprocessor.extract_features(gyr_file)

        println(s"found ${acc_features.count()} accelerometer features " +
          s"and ${gyr_features.count()} gyroscope features ")

        val struct = StructType(
            StructField("label", StringType) ::
            StructField("features", VectorType) :: Nil)

        val joined = acc_features.join(gyr_features)
        val data = ss.createDataFrame(joined.map(it => Row(it._1._4, Vectors.dense(it._2._1 ++ it._2._2))),  struct)

/* ******************************************** Multilayer perceptron
        val model = new classification.MLPClassifier(Array(18, 40, 6))
        val layers = Array(18,40,6)
        model.train(data, layers).save(classifier_params)
*/

/* ****************************************** Decision tree*/
       val model = new classification.DTClassifier()

      model.train(data)
 /**/

      scala.io.StdIn.readLine()
    }
}