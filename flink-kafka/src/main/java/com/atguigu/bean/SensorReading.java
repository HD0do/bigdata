package com.atguigu.bean;

public class SensorReading {
    private String name;
    private long timestamp;
    private double tm;

    public SensorReading(String name, long timestamp, double tm) {
        this.name = name;
        this.timestamp = timestamp;
        this.tm = tm;
    }

    public SensorReading() {
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "name='" + name + '\'' +
                ", timestamp=" + timestamp +
                ", tm=" + tm +
                '}';
    }
}

