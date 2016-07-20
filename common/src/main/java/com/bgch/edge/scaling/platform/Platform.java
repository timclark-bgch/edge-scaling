package com.bgch.edge.scaling.platform;

public final class Platform {
    private final PlatformConsumer consumer;
    private final MetricRecorder recorder;

    public Platform(final PlatformConsumer consumer, final MetricRecorder recorder) {
        this.consumer = consumer;
        this.recorder = recorder;

        this.consumer.registerHandler((ConnectHandler) connect -> this.recorder.connect());
        this.consumer.registerHandler((DisconnectHandler) disconnect -> this.recorder.disconnect());
        this.consumer.registerHandler((ReportHandler) report -> this.recorder.report());
    }

    public void start() {

    }

    public void stop()  {

    }
}
