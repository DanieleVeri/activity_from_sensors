package classification

import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel

class MLPModel(override val param_uri: String,
               override val rev_label_uri: String) extends
    Model(param_uri, rev_label_uri)
{
    override val model: MultilayerPerceptronClassificationModel =
        MultilayerPerceptronClassificationModel.load(param_uri)
}
