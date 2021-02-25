package com.atguigu.hot;


import com.atguigu.bean.ItemViewCount;
import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Properties;


public class HotItem {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取数据，创建DaTaStream
        DataStream<String> inputStream = env.readTextFile("D:\\develop\\5.java\\flink\\flink-request\\hotUserBehavior\\src\\main\\resources\\UserBehavior.csv");

        //3.转换数据类型
        DataStream<UserBehavior> dataStream = inputStream
                .map(line -> {
                    String[] fields = line.split(",");
                    return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior userBehavior) {
                        return userBehavior.getTimestamp() * 1000L;
                    }
                });

        //分组开窗聚合，得到每个窗口内各个商品的count值
        DataStream<ItemViewCount> windowAggStream = dataStream.filter(data -> "pv".equals(data.getBehavior()))  //过滤pv数据
                .keyBy("itemId")    //按商品Id分组
                .timeWindow(Time.hours(1), Time.minutes(5))   //开滑动窗口
                .aggregate(new ItemCountAgg(), new WindowItemCountResult());


        //收集每个窗口中所有商品的count数据，排序输出top n
        DataStream<String> resultSteam = windowAggStream
                .keyBy("windowEnd")
                .process(new TopNHotItems(5));

        resultSteam.print();
        env.execute("hot Items analysis");

    }
    /*
 自定义聚合函数
  */
    public static class ItemCountAgg implements AggregateFunction<UserBehavior,Long,Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator +1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a+b;
        }
    }

    /**
     * //自定义全窗口函数
     */
    public static class WindowItemCountResult implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow>{
        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) {

            Long itemId = tuple.getField(0);
            long windowEnd = window.getEnd();
            Long count = input.iterator().next();
            out.collect(new ItemViewCount(itemId, windowEnd, count));


        }
    }

    public static class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String>{

        // 定义属性，top n的大小
        private Integer topSize;

        private TopNHotItems(Integer topSize) {
            this.topSize = topSize;
        }

        @Override
        public void open(Configuration parameters) {

        }
        @Override
        public void processElement(ItemViewCount itemViewCount, Context context, Collector<String> collector) {

        }


        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) {}
    }
}
