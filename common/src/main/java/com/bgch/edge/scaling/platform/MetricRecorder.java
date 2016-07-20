package com.bgch.edge.scaling.platform;

public interface MetricRecorder {
    void connect();
    void disconnect();
    void report();
}
