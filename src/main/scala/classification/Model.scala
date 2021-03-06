package classification

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.{IndexToString, MinMaxScalerModel}
import org.apache.spark.sql.DataFrame

abstract class Model(val param_uri: String, val label_uri: String)
{
    val model: Transformer

    val label: IndexToString = IndexToString.load(label_uri + "_reverse")
    val scaler: MinMaxScalerModel = MinMaxScalerModel.load(param_uri + "_scaler" )

    def transform(data: DataFrame): DataFrame =
    {
        val scaled = scaler.transform(data)
        val result = model.transform(scaled)
        label.transform(result)
    }
}

object Model
{
    def get_model(kind: String, params_uri: String, label_uri: String): Model =
    {
        kind match {
            case "dt" =>
                new DTModel(params_uri, label_uri)
            case "mlp" =>
                new MLPModel(params_uri, label_uri)
            case _ => throw new IllegalArgumentException("Invalid model type")
        }
    }
}
