package com.bgch.edge.scaling.device;

public interface MetricRecorder {
    void connectSucceeded();
    void connectFailed();
    void reportSucceeded();
    void reportFailed();
    void commandReceived();
}
