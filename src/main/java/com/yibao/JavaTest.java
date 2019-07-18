package com.yibao;


import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;


/**
 * @author liuwenyi
 * @date 2019/07/17
 */
public class JavaTest {

    public static void main(String[] args) {

        //初始化spark应用
        SparkConf conf = new SparkConf().setAppName("wordcount").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //读取文件
        JavaRDD<String> lines = sc.textFile("data/test.txt");

        //将每一行切割成单词
        JavaRDD<String> words = lines.flatMap((FlatMapFunction<String, String>) line ->
                Lists.newArrayList(line.split(" ")).iterator());

        //将每个单词映射成(word,1)格式
        JavaPairRDD<String, Integer> pairs = words.mapToPair((PairFunction<String, String, Integer>) word -> new Tuple2<>(word, 1));

        //计算每个单词出现次数
        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey((Function2<Integer, Integer, Integer>) Integer::sum);

        //打印输出
        wordCounts.foreach((VoidFunction<Tuple2<String, Integer>>) wordCount -> System.out.println(wordCount._1 + " appeared " + wordCount._2 + " times."));

        //关闭SparkContext
        sc.close();

    }

}
