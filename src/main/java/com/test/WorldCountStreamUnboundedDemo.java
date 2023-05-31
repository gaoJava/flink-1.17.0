package com.test;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * DataStream实现wordcount 读socket 无界流
 */
public class WorldCountStreamUnboundedDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop100", 7778);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = socketDS
                .flatMap(
                        (String value, Collector<Tuple2<String, Integer>> collector) -> {
                            String[] words = value.split(" ");
                            for (String word : words) {
                                Tuple2<String, Integer> wordTuple2 = Tuple2.of(word, 1);
                                collector.collect(wordTuple2);
                            }
                        }
                )
                //不写会报错，flink里面类型不匹配
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(
                        (Tuple2<String, Integer> stringIntegerTuple2) -> {
                            return stringIntegerTuple2.f0;
                        }
                ).sum(1);
        sum.print();
        env.execute();

    }
}
