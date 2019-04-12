package kmeans

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

object KmeansML {

  def main(args: Array[String]) {

    /*
    if (args.length < 4) {
      println("Usage:KMeansClustering trainingDataFilePath testDataFilePath numClusters numIterations")
      sys.exit(1)
    }
    */

    val sparkConf = new SparkConf().setAppName("firstKmeans").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    /**
      * 数据样例：
      * Channel Region Fresh Milk Grocery Frozen Detergents_Paper Delicassen
      * 2       3      12669 9656 7561    214    2674             1338
      * 2       3      7057  9810 9568    1762   3293             1776
      * 2       3      6353  8808 7684    2405   3516             7844
      */

    /**
      * 第一部分：数据处理
      */
    val rawTrainingData = sc.textFile("D:\\myspark\\ml\\kmeans_customers_data2.csv")
    val trainingData =
      rawTrainingData.filter(!isColumnNameLine(_)).map(line => {
        //将一行数据转化为一个密集矩阵（本质是一个double类型的数组）
        Vectors.dense(line.split(",").map(_.trim).filter(!"".equals(_)).map(_.toDouble))
      }).cache()

    /**
      * 输入控制参数
      * 首先明确可以设定的参数
      * private var k: Int   聚类个数
      * private var maxIterations: Int   迭代次数
      * private var runs: Int,   运行kmeans算法的次数，spark2.0以后已废弃
      * private var initializationMode: String 初始化聚类中心的方法
      * 有两种选择 ① 默认方式 KMeans.K_MEANS_PARALLEL 即 KMeans|| ② KMeans.RANDOM 随机取
      * private var initializationSteps: Int 初始化步数
      * private var epsilon: Double  判断kmeans算法是否达到收敛的阈值 这个阈值由spark决定
      * private var seed: Long)private var k: Int 表示初始化时的随机种子
      */
    val numClusters:Int = 4 //簇的个数
    val numIterations = 20 //迭代次数
    var clusterIndex: Int = 0 //簇的索引

    /**
      * 第二部分：训练聚类模型
      * 参数一：被训练的数据集
      * 参数二：最后聚类的个数
      * 参数三：迭代次数
      * 还可以指定其他参数：initializationMode，initializationSteps，seed
      */
    val model: KMeansModel = KMeans.train(trainingData, numClusters, numIterations)

    //查看聚类的个数，即传入的k
    println("Spark MLlib K-means clustering starts ....")
    println("Cluster Number:" + model.clusterCenters.length)

    println("Cluster Centers Information Overview:")
    //聚类中心点：
    //Center Point of Cluster 0:
    //[1.4107142857142856,2.669642857142857,5378.571428571428,6492.169642857142,8986.196428571428,2134.866071428571,3617.2589285714284,1131.6339285714284]
    //Center Point of Cluster 1:
    //[2.0,3.0,29862.5,53080.75,60015.75,3262.25,27942.25,3082.25]
    //Center Point of Cluster 2:
    //[1.1111111111111112,3.0,41495.555555555555,2769.111111111111,3744.4444444444443,4261.888888888889,948.6666666666666,3041.111111111111]
    //Center Point of Cluster 3:
    //[1.2280701754385963,2.807017543859649,19729.22807017544,4192.9473684210525,5462.666666666666,3237.9649122807014,1286.4912280701753,1912.8245614035086]
    model.clusterCenters.foreach(
      x => {
        println("Center Point of Cluster " + clusterIndex + ":")
        println(x)
        clusterIndex += 1
      })

    /**
      * 第三部分：根据聚类结果开始检查每个点属于哪个簇
      */
    //val rawTestData = sc.textFile(args(1))
    //val parsedTestData = rawTestData.map(line => {
    //  Vectors.dense(line.split(",")
    //    .map(_.trim).filter(!"".equals(_))
    //    .map(_.toDouble))
    //})
    trainingData.collect().foreach(lineData => {
      val predictedClusterIndex: Int = model.predict(lineData)
      println("The data " + lineData.toString + " belongs to cluster " + predictedClusterIndex)
    })

    println("Spark MLlib K-means clustering finished ....")
  }

  /**
    * 根据关键字过滤第一行
    * @param line 输入行
    * @return
    */
  private def isColumnNameLine(line: String): Boolean = {
    if (line != null && line.contains("Channel")) true
    else false
  }
}