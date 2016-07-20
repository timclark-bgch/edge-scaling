package com.bgch.edge.scaling.metrics;

import com.bgch.edge.scaling.platform.MetricRecorder;

public final class PlatformRecorder implements MetricRecorder {
    @Override
    public void connect() {
        System.out.println("C");
    }

    @Override
    public void disconnect() {
        System.out.println("D");
    }

    @Override
    public void report() {
        System.out.println("R");
    }
}
