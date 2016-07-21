package com.bgch.edge.scaling.metrics;

import com.bgch.edge.scaling.device.MetricRecorder;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.TimeUnit;

public final class DeviceRecorder implements MetricRecorder {
    private final MetricRegistry metrics = new MetricRegistry();
    private final Counter connectSuccess = metrics.counter("device.connect.success");
    private final Counter connectFailed = metrics.counter("device.connect.failed");
    private final Meter reportSucceeded = metrics.meter("device.report.success");
    private final Counter reportFailed = metrics.counter("device.disconnect.failed");
    private final Meter commandReceived = metrics.meter("device.command.success");

    public DeviceRecorder() {
        final ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics).convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.SECONDS).build();
        reporter.start(10, TimeUnit.SECONDS);
    }

    @Override
    public void connectSucceeded() {
        connectSuccess.inc();
    }

    @Override
    public void connectFailed() {
        connectFailed.inc();
    }

    @Override
    public void reportSucceeded() {
        reportSucceeded.mark();
    }

    @Override
    public void reportFailed() {
        reportFailed.inc();
    }

    @Override
    public void commandReceived() {
        commandReceived.mark();
    }
}
