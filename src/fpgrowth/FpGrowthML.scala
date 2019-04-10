package fpgrowth

import java.util.Properties

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object FpGrowthML {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("FPGrowthTest")
      .setMaster("local")
    //.set("spark.sql.warehouse.dir","E:/ideaWorkspace/ScalaSparkMl/spark-warehouse")
    val sparkContext = new SparkContext(sparkConf)

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    import spark.implicits._

    //设置参数
    val minSupport = 0.2 //最小支持度，只能在0~1之间
    val minConfidence = 0.8 //最小置信度，只能在0~1之间
    val numPartitions = 2 //数据分区

    //取出数据
    val data = sparkContext.textFile("D:\\myspark\\ml\\fpgrowth2.txt")

    //把数据通过空格分割
    val transactions = data.map(x => x.split(" "))
    //缓存一下，因为fpgrowth需要扫描两次原始事务记录
    transactions.cache()

    //创建一个FPGrowth的算法实列
    val fpg = new FPGrowth()
    //设置训练时候的最小支持度和数据分区
    fpg.setMinSupport(minSupport).setNumPartitions(numPartitions)

    //把数据带入算法中
    val model = fpg.run(transactions)

    //查看所有的频繁项集，并且列出它出现的次数
    println("所有的频繁项集：")
    //model.freqItemsets 本质是一个数组：
    //Array({t}: 3, {t,x}: 3, {t,x,z}: 3, {t,z}: 3, {s}: 3, {s,t}: 2, ....}
    //foreach遍历每一项记为freqItemRecord
    //{t}: 3
    //{t,x}: 3
    //{t,x,z}: 3
    //{t,z}: 3
    //freqItemRecord.items 对应前面的{t,x,z}，本质也是一个数组Array(t,x,z)
    //freqItemRecord.freq 对应后面的3，表示该项出现的频次
    model.freqItemsets.collect().foreach(
      freqItemRecord=>{
      println(freqItemRecord.items.mkString("[", ",", "]") + "," + freqItemRecord.freq)
    })

    //通过置信度筛选出推荐规则
    //自动产生rdd的格式为{q,z} => {t}: 1.0
    //antecedent表示前项，本质也是一个数组Array(q, z)，可以有一个或多个元素
    //consequent表示后项，本质也是一个数组Array(t)，但只可以有一个元素
    //confidence表示该条规则的置信度
    //需要注意的是：
    //1.所有的规则产生的推荐，后项只有1个，相同的前项产生不同的推荐结果是不同的行
    //2.不同的规则可能会产生同一个推荐结果，所以样本数据过规则的时候需要去重
    //model.generateAssociationRules(minConfidence).collect().foreach(rule => {
    //  println(rule.antecedent.mkString(",") + "-->" +
    //    rule.consequent.mkString(",") + "-->" + rule.confidence)
    //})

    /**
      * 下面的代码是把规则写入到Mysql数据库中，以后使用来做推荐
      * 如果规则过多就把规则写入redis，这里就可以直接从内存中读取了
      */
    //新建一个可变数据，用来存放分析出的规则
    val ruleArray = new ArrayBuffer[Array[String]]()

    //本质一个rule是 Array(a,b) => Array(c) : 0.8
    model.generateAssociationRules(minConfidence).collect().foreach(
      rule => {
        val preOrder = rule.antecedent.mkString(",")
        val sufOrder = rule.consequent(0)
        val confidence = rule.confidence.toString
        //val ruleList:List[String] = preOrder::sufOrder::confidence::Nil
        ruleArray += Array(preOrder, sufOrder, confidence)
        //ruleArray.map(parts=>KnowLedge(parts(0),parts(1),parts(2)))
    })

    //将分析出的规则转入为df，并存入mysql数据库
    val ruleDF = ruleArray.map(elem => KnowLedge(elem(0),elem(1),elem(2).toDouble)).toDF()

    //存放mysql配置
    val prop = new Properties
    prop.setProperty("user","root")
    prop.setProperty("password","123456")

    ruleDF.write.mode("append").jdbc("jdbc:mysql://hadoop01:3306/packmas", "t_item_index", prop)

    //查看规则生成的数量
    //println(model.generateAssociationRules(minConfidence).collect().length)
  }

  /**
    * 样例类：知识点关联表
    * @param perOrder 前序知识点
    * @param sufOrder 后续知识点
    * @param confidence 置信度
    */
  case class KnowLedge(perOrder:String, sufOrder:String, confidence:Double)
}
