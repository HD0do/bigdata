package com.atguigu.defineSource;

import com.atguigu.bean.SensorReading;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

public class MySensor implements SourceFunction<SensorReading> {


    private boolean running=true;
    @Override
    public void run(SourceContext<SensorReading> ctx) throws Exception {
        Random random =new Random();

        HashMap<String , Double> sensorTempMap=new HashMap<>();

        for (int i = 0; i < 10 ; i++) {
            sensorTempMap.put("sensor_"+(i+1),60 + random.nextGaussian()*20);

        }

        while(running){
            for (String sensorId:sensorTempMap.keySet()){
                double newTemp=sensorTempMap.get(sensorId)+random.nextGaussian();

                sensorTempMap.put(sensorId,newTemp);
                ctx.collect(new SensorReading(sensorId,System.currentTimeMillis(),newTemp));
            }

            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {


        this.running=false;
    }
}
