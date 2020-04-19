package classification
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.DataFrame


trait Classifier[T >: Transformer]
{
  def train(data: DataFrame): T
  def load_model(params_uri: String): T
}
