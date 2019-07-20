package com.yibao;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.LongAccumulator;


import java.util.List;

/**
 * @author liuwenyi
 * @date 2019/07/20
 */
public class AccumulatorVariable {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("AccumulatorVariable").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> integers = Lists.newArrayList(1, 2, 3, 4);
        JavaRDD<Integer> parallelize = sc.parallelize(integers);
        LongAccumulator longAccumulator = sc.sc().longAccumulator("Test");
        parallelize.foreach((VoidFunction<Integer>) longAccumulator::add);
        System.out.println(longAccumulator.value());
    }
}
