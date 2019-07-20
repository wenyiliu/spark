package com.yibao;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.List;

/**
 * @author liuwenyi
 * @date 2019/07/20
 */
public class BroadCastVariable {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("BroadCastVariable");

        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Lists.newArrayList(1, 2, 3, 4, 5, 6);
        JavaRDD<Integer> parallelize = sc.parallelize(list);
        final int i = 3;
        Broadcast<Integer> broadcast = sc.broadcast(i);
        JavaRDD<Integer> map = parallelize.map(new Function<Integer, Integer>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(Integer integer) throws Exception {
                return integer * broadcast.value();
            }
        });
        map.foreach(new VoidFunction<Integer>() {
            private static final long serialVersionUID = 1L;

            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
        sc.close();
    }
}
