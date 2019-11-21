/*
 * Copyright 2015 and onwards Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
 *
 * See LICENSE file for further information.
 */

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Random

object RunKMeans {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    val data = spark.read.
      option("inferSchema", true).
      option("header", false).
      csv("ch05/kddcup.data.corrected").
      toDF(
        "duration", "protocol_type", "service", "flag",
        "src_bytes", "dst_bytes", "land", "wrong_fragment", "urgent",
        "hot", "num_failed_logins", "logged_in", "num_compromised",
        "root_shell", "su_attempted", "num_root", "num_file_creations",
        "num_shells", "num_access_files", "num_outbound_cmds",
        "is_host_login", "is_guest_login", "count", "srv_count",
        "serror_rate", "srv_serror_rate", "rerror_rate", "srv_rerror_rate",
        "same_srv_rate", "diff_srv_rate", "srv_diff_host_rate",
        "dst_host_count", "dst_host_srv_count",
        "dst_host_same_srv_rate", "dst_host_diff_srv_rate",
        "dst_host_same_src_port_rate", "dst_host_srv_diff_host_rate",
        "dst_host_serror_rate", "dst_host_srv_serror_rate",
        "dst_host_rerror_rate", "dst_host_srv_rerror_rate",
        "label")

    data.cache()

    val runKMeans = new RunKMeans(spark)

    runKMeans.clusteringTake0(data)


    data.unpersist()
  }

}

class RunKMeans(private val spark: SparkSession) {

  import spark.implicits._

  // Clustering, Take 0

  def clusteringTake0(data: DataFrame): Unit = {

    data.select("label").groupBy("label").count().orderBy($"count".desc).show(25)

    val numericOnly = data.drop("protocol_type", "service", "flag").cache()

    val assembler = new VectorAssembler().
      setInputCols(numericOnly.columns.filter(_ != "label")).
      setOutputCol("featureVector")

    val kmeans = new KMeans().
      setSeed(Random.nextLong()).
      setPredictionCol("cluster").
      setFeaturesCol("featureVector")

    val pipeline = new Pipeline().setStages(Array(assembler, kmeans))
    val pipelineModel = pipeline.fit(numericOnly)
    val kmeansModel = pipelineModel.stages.last.asInstanceOf[KMeansModel]

    kmeansModel.clusterCenters.foreach(println)

    val withCluster = pipelineModel.transform(numericOnly)

    withCluster.select("cluster", "label").
      groupBy("cluster", "label").count().
      orderBy($"cluster", $"count".desc).
      show(25)

    numericOnly.unpersist()
  }


}