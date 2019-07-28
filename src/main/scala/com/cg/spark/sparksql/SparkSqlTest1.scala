package com.cg.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object SparkSqlTest1 {
  def main(args: Array[String]): Unit = {
    // 1.创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().appName(name = "SparkSqlTest1").master(master = "local").getOrCreate();
    // 2.通过SparkSession获取SparkContext
    val sc = spark.sparkContext
    // 3.加载数据文件
    val lines: RDD[String] = sc.textFile("d:/PVUV.txt")
    // 4.对每一行数据切分
    val attr: RDD[Array[String]] = lines.map(line=>line.split(","))
    // 5.将rdd与样例类关联
    val userVisitRDD: RDD[UserVisit] = attr.map(item=>UserVisit(item(0).toString(), item(1).toString(), item(2).toString(), item(3).toString(), item(4).toString(), item(5).toString(), item(6).toString()))
    // 导入隐式转换
    import spark.implicits._
    // 6.将RDD转为DataFrame
    val df: DataFrame = userVisitRDD.toDF()
    df.printSchema()
    
    
    df.show()
    // 查询某个字段
    df.select("url").show()
    
    // 
    // df.select($"action" == "buy").show()
    
    
    spark.stop()
  }
}

case class UserVisit(ipaddress: String, province: String, date: String, token: String, userId: String, url: String, action: String)