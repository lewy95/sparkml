package kmeans

import scala.collection.mutable
import scala.io.Source
import scala.util.Random

object KmeansScala {
  val k = 2 //簇的个数
  val dim = 3 //数据集维度(1.2 2.0 3.2)即三维
  val shold = 0.0000000001 //阈值，用于判断聚类中心偏移量
  val centers = new Array[Vector[Double]](k) //聚类中心点（迭代更新）
  def main(args: Array[String]): Unit = {
    //处理输入数据
    val fileName = "D:\\myspark\\ml\\kmark.txt"
    val lines = Source.fromFile(fileName).getLines()
    val points = lines.map(line => {
      //数据预处理，将每一行都存储到一个vector向量中
      val parts = line.split(" ").map(_.toDouble)
      var vector = Vector[Double]()
      for (i <- 0 until dim)
        vector ++= Vector(parts(i))
      vector
    }).toArray
    //最后points的结构应该是：([1.0,1.1,1.2],[1.2,3.2,5.2],....)

    /**
      * 随机初始化聚类中心
      * 由于k为2，所以聚类中心只有两个
      * 如([6.2, 6.2, 6.2],[5.1, 5.1, 5.1])
      */
    initialCenters(points)

    /**
      * 迭代做聚类
      */
    kmeans(points, centers)

    /**
      * 输出聚类结果
      */
    printResult(points, centers)
  }

  //--------------随机初始化聚类中心----------------
  def initialCenters(points: Array[Vector[Double]]):Unit = {
    //获取数据集个数
    val pointsNum = points.length //30
    //寻找k个随机数(作为数据集的下标)，并选取它们而初始化中心点
    var index = 0
    var flag = true
    var temp = 0
    //用一个数组保存随机的数据集下标号
    var array = new mutable.MutableList[Int]()
    //产生随机数的个数和k相等
    while (index < k) {
      //产生一个随机数，不包括pointsNum，最大到pointsNum - 1
      temp = new Random().nextInt(pointsNum)
      flag = true
      //判断是否存在于数组中，没有则增加
      //这里避免了初始随机选择到重复聚类中心
      if (array.contains(temp)) {
        //在数组中存在
        flag = false
      }
      else {
        if (flag) {
          array = array :+ temp
          index += 1
        }
      }
    }
    for (i <- centers.indices) {
      centers(i) = points(array(i))
      println("初始化中心点如下：")
      println("line index: " + (array(i) + 1))
      println(centers(i))
    }
  }

  //---------------------------迭代做聚类-------------------------------------
  def kmeans(points: Array[Vector[Double]], centers: Array[Vector[Double]]):Unit = {
    var bool = true
    //存放新的聚类中心
    var newCenters = Array[Vector[Double]]()
    var move = 0.0
    var currentCost = 0.0 //当前的代价函数值
    var newCost = 0.0
    //根据每个样本点最近的聚类中心进行groupBy分组
    //最后得到的cluster结构是：Map[Vector[Double],Array[Vector[Double]]]
    //Map中的key就是聚类中心，value就是依赖于该聚类中心的点集
    while (bool) {
      //迭代更新聚类中心，直到最优
      move = 0.0
      //计算当前代价函数（初始化随机聚类中心后，第一次计算）
      currentCost = computeCost(points, centers)
      //进行聚类，将各样本点归属给最近的聚类中心
      //groupBy方法：按指定规则做聚合，最后将结果保存到一个map里
      val cluster = points.groupBy(v => closestCenter(centers, v))
      //找到新的聚类中心
      newCenters =
        centers.map(oldCenter => {
          cluster.get(oldCenter) match {
            //找到该聚类中心所拥有的点集
            //通过option的模式匹配判断key对应的value是否有值：Some表示有值;None表示没值
            //也就是判断当前聚类中心下有归属它的点
            case Some(pointsInThisCluster) =>
              //有归属于它的点集，将它们的均值作为新的聚类中心
              //计算过程：
              //1.把所有点集进行相关，即：
              //([1.2,1.4,1.6],[2.0,3.2,2.0] => [3.2,4.6,3.6])
              //2.除以点集的个数
              //[3.2,4.6,3.6] / 2 = [3.2/2,4.6/2,3.6/2] = [1.6,2.3,1.8]
              vectorDivide(pointsInThisCluster.reduceLeft((v1, v2) => vectorAdd(v1, v2)), pointsInThisCluster.length)
            case None => oldCenter
          }
        })
      //添加到centers集合中，作为下一次聚类的中心
      for (i <- centers.indices) {
        centers(i) = newCenters(i)
      }

      //计算得到新的代价函数
      newCost = computeCost(points, centers)
      println("当前代价函数值：" + currentCost)
      println("新的代价函数值：" + newCost)
      //如果每个样本点到聚类中心的距离之和不再有很大变化
      //这里指定了一个阈值，一旦小于这个阈值则不会继续迭代
      if (math.sqrt(vectorDis(Vector(currentCost), Vector(newCost))) < shold)
        bool = false
    }
    //退出迭代后，说明此时的代价函数已是最小，那么此时的聚类中心也是最优的
    println("寻找到的最优中心点如下：")
    for (i <- centers.indices) {
      println(centers(i))
    }
  }

  //--------------计算代价函数（直到每个样本点到聚类中心的距离之和不再有很大变化）--------------
  def computeCost(points: Array[Vector[Double]], centers: Array[Vector[Double]]): Double = {
    //找到某样本点所属的簇
    //cluster:Map[Vector[Double],Array[Vector[Double]]
    //groupBy方法：按指定规则做聚合，最后将结果保存到一个map里
    val cluster = points.groupBy(v => closestCenter(centers, v))
    //记录代价函数
    var costSum = 0.0
    for (i <- centers.indices) {
      cluster.get(centers(i)) match {
        case Some(subSets) =>
          for (j <- centers.indices) {
            //进行计算，即所有样本点到其所属簇中心的距离的平方和
            costSum += (vectorDis(centers(i), subSets(j)) * vectorDis(centers(i), subSets(j)))
          }
        case None => costSum = costSum
      }
    }
    costSum
  }

  //-------------------找到某样本点所属的聚类中心------------------------
  def closestCenter(centers: Array[Vector[Double]], v: Vector[Double]): Vector[Double] = {
    centers.reduceLeft((c1, c2) =>
      if (vectorDis(c1, v) < vectorDis(c2, v)) c1 else c2
    )
  }

  //--------------------------输出聚类结果-----------------------------
  def printResult(points: Array[Vector[Double]], centers: Array[Vector[Double]]):Unit = {
    val pointsNum = points.length
    //存放聚类标签，长度为总的记录数
    val pointsLabel = new Array[Int](pointsNum)
    //存放聚类中心
    var closetCenter = Vector[Double]()
    println("聚类结果如下：")
    for (i <- 0 until pointsNum) {
      closetCenter = centers.reduceLeft((c1, c2) =>
        if (vectorDis(c1, points(i)) < vectorDis(c2, points(i))) c1 else c2)
      //将每个点的聚类中心用centers中的下标表示，属于同一类的点拥有相同的下标
      pointsLabel(i) = centers.indexOf(closetCenter)
      println(points(i) + "-----------" + pointsLabel(i))
    }
  }

  //--------------------------自定义向量间的运算-----------------------------
  //--------------------------向量间的欧式距离-----------------------------
  def vectorDis(v1: Vector[Double], v2: Vector[Double]): Double = {
    var distance = 0.0
    for (i <- v1.indices) {
      // (v1(0)-v2(0))^2 + (v1(1)-v2(1))^2
      distance += (v1(i) - v2(i)) * (v1(i) - v2(i))
    }
    distance = math.sqrt(distance)
    distance
  }

  //--------------------------向量加法-----------------------------
  def vectorAdd(v1: Vector[Double], v2: Vector[Double]): Vector[Double] = {
    var v3 = v1
    for (i <- v1.indices) {
      //updated方法：def updated(index: Int, elem: A): Vector[A]
      //a copy of this vector with the element at position index replaced by elem.
      //即返回一个修改索引元素后原集合的备份
      v3 = v3.updated(i, v1(i) + v2(i))
    }
    v3
  }

  //--------------------------向量除法-----------------------------
  def vectorDivide(v: Vector[Double], num: Int): Vector[Double] = {
    var r = v
    for (i <- v.indices) {
      r = r.updated(i, r(i) / num)
    }
    r
  }
}
