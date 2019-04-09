package kmeans

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

object KmeansML {

  def main(args: Array[String]) {
    if (args.length < 4) {
      println("Usage:KMeansClustering trainingDataFilePath testDataFilePath numClusters numIterations")
      sys.exit(1)
    }

    val conf = new
        SparkConf().setAppName("first kmeans")
    val sc = new SparkContext(conf)

    /**
      * 数据样例：
      * Channel Region Fresh Milk Grocery Frozen Detergents_Paper Delicassen
      * 2       3      12669 9656 7561    214    2674             1338
      * 2       3      7057  9810 9568    1762   3293             1776
      * 2       3      6353  8808 7684    2405   3516             7844
      */

    /**
      * 第一部分：训练模型
      */
    val rawTrainingData = sc.textFile(args(0))
    val parsedTrainingData =
      rawTrainingData.filter(!isColumnNameLine(_)).map(line => {
        //将一行数据转化为一个密集矩阵（本质是一个double类型的数组）
        Vectors.dense(line.split(",").map(_.trim).filter(!"".equals(_)).map(_.toDouble))
      }).cache()

    //输入控制参数
    val numClusters = args(2).toInt //簇的个数
    val numIterations = args(3).toInt //迭代次数
    //val runTimes = args(4).toInt
    var clusterIndex: Int = 0  //簇的索引

    //训练kmeans聚类模型
    //val clusters: KMeansModel = KMeans.train(parsedTrainingData, numClusters, numIterations, runTimes)
    val clusters: KMeansModel = KMeans.train(parsedTrainingData, numClusters, numIterations)

    //Cluster Number:8
    println("Cluster Number:" + clusters.clusterCenters.length)

    //Center Point of Cluster 0:
    //[1.1363636363636365,2.659090909090909,32837.52272727273,4951.022727272727,5582.090909090909,4236.727272727273,955.2272727272727,1956.2954545454545]
    //Center Point of Cluster 1:
    //[1.0903614457831325,2.524096385542169,4878.734939759036,2856.7289156626507,3352.2590361445787,2541.024096385542,948.7349397590361,891.3493975903615]
    //Center Point of Cluster 2:
    //[1.0,2.5,34782.0,30367.0,16898.0,48701.5,755.5,26776.0]
    //Center Point of Cluster 3:
    //[2.0,2.8000000000000003,25603.0,43460.600000000006,61472.200000000004,2636.0,29974.2,2708.8]
    //Center Point of Cluster 4:
    //[2.0,2.4736842105263155,8219.894736842105,20841.105263157893,29019.0,2018.157894736842,12900.842105263157,3307.7894736842104]
    //Center Point of Cluster 5:
    //[1.0,2.4285714285714284,68409.71428571428,7298.857142857142,8161.0,11348.42857142857,1409.4285714285713,3061.0]
    //Center Point of Cluster 6:
    //[1.209090909090909,2.5454545454545454,16440.53636363636,3011.0636363636363,4397.236363636363,3525.9181818181814,1017.0181818181818,1451.8545454545454]
    //Center Point of Cluster 7:
    //[1.8505747126436782,2.528735632183908,4417.540229885058,9218.022988505747,14518.781609195403,1462.1379310344828,6323.0,1446.287356321839]
    println("Cluster Centers Information Overview:")
    clusters.clusterCenters.foreach(
      x => {

        println("Center Point of Cluster " + clusterIndex + ":")

        println(x)
        clusterIndex += 1
      })

    /**
      * 第二部分：根据聚类结果开始检查每个测试数据属于哪个集群
      */
    val rawTestData = sc.textFile(args(1))
    val parsedTestData = rawTestData.map(line => {

      Vectors.dense(line.split(",").map(_.trim).filter(!"".equals(_)).map(_.toDouble))

    })
    parsedTestData.collect().foreach(testDataLine => {
      val predictedClusterIndex: Int = clusters.predict(testDataLine)

      println("The data " + testDataLine.toString + " belongs to cluster " +
        predictedClusterIndex)
    })

    println("Spark MLlib K-means clustering test finished.")
  }

  private def isColumnNameLine(line: String): Boolean = {
    if (line != null && line.contains("Channel")) true
    else false
  }
}
