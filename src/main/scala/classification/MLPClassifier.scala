package classification

import org.apache.spark.ml.classification.{MultilayerPerceptronClassificationModel, MultilayerPerceptronClassifier}

class MLPClassifier(override val seed: Long, val layers: Array[Int]) extends
    Classifier[MultilayerPerceptronClassificationModel](seed)
{
    override val trainer: MultilayerPerceptronClassifier =
        new MultilayerPerceptronClassifier()
            .setLayers(layers)
            .setBlockSize(128)
            .setMaxIter(100)
            .setSeed(seed)
}