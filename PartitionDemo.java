package com.sankir;
import org.apache.spark.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


public class PartitionDemo {
    public static void main(String[] args)  {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Sankir");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        LongAccumulator acc = jsc.sc().longAccumulator();   // Initialise the accumulator.


        final Broadcast<Integer> bc = jsc.broadcast(5);
        System.out.println("Broadcast variable: " + bc.value());

        JavaRDD<String> RDD1 = jsc.textFile("E:\\ExampleForCombineByKey.txt");


        System.out.println(RDD1.collect());

        JavaPairRDD<String,Integer> RDD2 = RDD1.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String category = s.split("\t")[0];
                Integer clicks = Integer.parseInt(s.split("\t")[1]);
                if (category.startsWith("H"))
                    acc.add(1);
                return new Tuple2<>(category, clicks + bc.value());
            }
        });

        System.out.println("Accumulator value before collect(): " + acc.value());
        System.out.println(RDD2.collect());
        System.out.println("Accumulator value after collect(): " + acc.value());



        HashPartitioner hp = new HashPartitioner(3);


        JavaPairRDD<Integer,Integer> rdd1 = jsc.parallelizePairs(Arrays.asList(new Tuple2<Integer,Integer>(1,10),
                new Tuple2<Integer,Integer>(2,10),
                new Tuple2<Integer,Integer>(3,10),
                new Tuple2<Integer,Integer>(4,10),
                new Tuple2<Integer,Integer>(5,10),
                new Tuple2<Integer,Integer>(6,10),
                new Tuple2<Integer,Integer>(7,10),
                new Tuple2<Integer,Integer>(8,10),
                new Tuple2<Integer,Integer>(9,10),
                new Tuple2<Integer,Integer>(10,10)));

        System.out.println("Output of rdd1");
        System.out.println(rdd1.collect());
        System.out.println("Partitions (Before): " + rdd1.partitions().size());

        String s = new String("SANKIRTECHNOLOGIES");
        System.out.println("Hashcode: " + s.hashCode() + "Partition: " + s.hashCode() % 3 );

        JavaPairRDD<Integer,Integer> partRDD = rdd1.partitionBy(hp);
        System.out.println("Partitions(After partitioning): " + partRDD.partitions().size());

        JavaRDD<String> mapPartRDD = partRDD.mapPartitionsWithIndex((index, tupleIterator) -> {
            List<String> list=new ArrayList<>();
            while(tupleIterator.hasNext()){
                list.add("Partition number:"+index+",key:"+ tupleIterator.next()._1());
            }
            return list.iterator();
        }, false);
        System.out.println(mapPartRDD.collect());

        JavaRDD<String> repartRDD = mapPartRDD.repartition(5);
        System.out.println(repartRDD.partitions().size());

        JavaRDD<String> mapRDD = RDD1.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
            @Override
            public Iterator<String> call(Iterator<String> strIter) throws Exception {
                List<String> outList = new ArrayList<String>();
                while(strIter.hasNext()) {
                    String currentObj = strIter.next();
                    outList.add("NEW " + currentObj);
                }
                return  (outList.iterator());
            }
        });


        System.out.println("RDD after mapPartitions:");
        System.out.println(mapRDD.collect());

        System.out.println("Range Partitioning:");

        JavaPairRDD<Integer, String> pairRdd = jsc.parallelizePairs(Arrays.asList(new Tuple2<>(1, "A"),
                new Tuple2<>(2, "B"),new Tuple2<>(3, "C"),
                new Tuple2<>(4, "D"),new Tuple2<>(5, "E"),
                new Tuple2<>(6, "F"),new Tuple2<>(7, "G"),
                new Tuple2<>(8, "H"),new Tuple2<>(9, "A"),
                new Tuple2<>(10,"B"),new Tuple2<>(11, "A"),
                new Tuple2<>(12,"B")));

        RDD<Tuple2<Integer, String>> rdd = JavaPairRDD.toRDD(pairRdd);


        System.out.println("Before partitioning : " + pairRdd.partitions().size());

        RangePartitioner rangePartitioner = new RangePartitioner(4, rdd, true, scala.math.Ordering.Int$.MODULE$ , scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        JavaPairRDD<Integer, String> rangePartRDD = pairRdd.partitionBy(rangePartitioner);

        pairRdd.collect();
        System.out.println("After partitioning: " + rangePartRDD.partitions().size());
        JavaRDD<String> mapPartRDD2 = rangePartRDD.mapPartitionsWithIndex((index, tupleIterator) -> {
            List<String> list=new ArrayList<>();
            while(tupleIterator.hasNext()){
                list.add("Partition number:"+index+",key:"+tupleIterator.next()._1());
            }
            return list.iterator();
        }, true);

        System.out.println(mapPartRDD2.collect());


    }
}