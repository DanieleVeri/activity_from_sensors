package classification

import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel

class MLPModel(override val param_uri: String,
               override val label_uri: String) extends
    Model(param_uri, label_uri)
{
    override val model: MultilayerPerceptronClassificationModel =
        MultilayerPerceptronClassificationModel.load(param_uri)
}
