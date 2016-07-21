package com.bgch.edge.scaling.metrics;

import com.bgch.edge.scaling.platform.MetricRecorder;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.TimeUnit;

public final class PlatformRecorder implements MetricRecorder {
    private final MetricRegistry metrics = new MetricRegistry();
    private final Meter connects = metrics.meter("platform.connects");
    private final Meter disconnects = metrics.meter("platform.disconnects");
    private final Meter reports = metrics.meter("platform.reports");

    public PlatformRecorder()   {
        final ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics).convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.SECONDS).build();
        reporter.start(10, TimeUnit.SECONDS);
    }
    @Override
    public void connect() {
//        System.out.println("C");
        connects.mark();
    }

    @Override
    public void disconnect() {
//        System.out.println("D");
        disconnects.mark();
    }

    @Override
    public void report() {
//        System.out.println("R");
        reports.mark();
    }
}
