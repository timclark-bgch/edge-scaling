package com.bgch.edge.scaling.device;

import honeycomb.messages.MessageProtos;

import java.util.List;

public class Device {
    private final String id;
    private final List<String> managed;
    private final DevicePublisher publisher;
    private final DeviceConsumer consumer;

    public Device(final String id, final List<String> managed, final DevicePublisher publisher, final DeviceConsumer consumer) {
        this.id = id;
        this.managed = managed;
        this.publisher = publisher;
        this.consumer = consumer;
    }

    public void start() {
        record(publisher.connect(MessageProtos.Connect.newBuilder().setDevice(id).addAllManaged(managed).build()),
                connectSucceeded,
                connectFailed);

        consumer.registerHandler(handler);
    }

    public void stop() {
        publisher.disconnect(MessageProtos.Disconnect.newBuilder().setDevice(id).build());
    }

    public void report() {
        record(publisher.report(MessageProtos.Report.newBuilder().setDevice(id).setMessage("test").build()),
                reportSucceeded, reportFailed);
        managed.forEach(device -> {
            record(publisher.report(MessageProtos.Report.newBuilder().setDevice(device).setMessage("test").build()), reportSucceeded, reportFailed);
        });
    }

    private final CommandHandler handler = command -> {
        System.out.printf("Command received %s\n", command.getDevice());
        // TODO increment correct metric
    };

    private void record(final boolean predicate, final Action success, final Action failure) {
        if (predicate) {
            success.execute();
        } else {
            failure.execute();
        }
    }

    private Action connectSucceeded = () -> {
        // TODO increment correct metric
    };

    private Action connectFailed = () -> {
        // TODO increment correct metric
    };

    private Action reportSucceeded = () -> {
        // TODO increment correct metric
    };

    private Action reportFailed = () -> {
        // TODO increment correct metric
    };

}
