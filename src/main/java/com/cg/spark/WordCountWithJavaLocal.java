//package com.cg.spark;
//
//import java.util.Arrays;
//import java.util.Iterator;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.api.java.function.Function2;
//import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.api.java.function.VoidFunction;
//
//import scala.Tuple2;
//
///**
// * @author rex.tan
// * @date 2019年7月28日 上午1:36:22
// */
//public class WordCountWithJavaLocal {
//
//	public static void main(String[] args) {
//		// 1.创建SparkConf对象
//		SparkConf conf = new SparkConf().setAppName("spark_test");
//		// 2.创建JavaSparkContext对象
//		JavaSparkContext sc = new JavaSparkContext(conf);
//		// 3.加载数据文件
//		JavaRDD<String> lines = sc.textFile(args[0]);
//		// 4.对文本切分、压平
//		// 第一个参数是输入数据, 第二个是输入数据
//		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>(){
//			private static final long serialVersionUID = 1L;
//
//			public Iterator<String> call(String line) throws Exception {
//				// 对每一行切分
//				String[] arr = line.split(" ");
//				return Arrays.asList(arr).iterator();
//			}
//		});
//		// 5.对所有的单词进行统计, 每个单词出现一次记为1
//		// 第一个参数是输入类型, 第二个是返回元组的key的类型, 第三个是返回元组的value类型
//		JavaPairRDD<String, Integer>  pairs = words.mapToPair(new PairFunction<String, String, Integer>(){
//			private static final long serialVersionUID = 1L;
//
//			public Tuple2<String, Integer> call(String word) throws Exception {
//				return new Tuple2<String, Integer>(word, 1);
//			}
//		});
//		// 6.对相同单词进行统计
//		// 第一个和第二个是输入类型, 第三个是返回类型
//		JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>(){
//			private static final long serialVersionUID = 1L;
//
//			public Integer call(Integer v1, Integer v2) throws Exception {
//				return v1 + v2;
//			}
//		});
//		// 7.保存结果
//		wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>(){
//			private static final long serialVersionUID = 1L;
//
//			public void call(Tuple2<String, Integer> tuple) throws Exception {
//				System.out.println(tuple);
//			}
//		});
//		
//		
//		
//		sc.close();
//		
//	}
//}
