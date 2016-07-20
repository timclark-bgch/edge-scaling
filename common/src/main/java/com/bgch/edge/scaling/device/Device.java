package com.bgch.edge.scaling.device;

import com.bgch.edge.scaling.Action;
import honeycomb.messages.MessageProtos;

import java.util.List;

public class Device {
    private final String id;
    private final List<String> managed;
    private final DevicePublisher publisher;
    private final DeviceConsumer consumer;
    private final MetricRecorder recorder;
    private final CommandHandler handler;

    public Device(final String id, final List<String> managed, final DevicePublisher publisher, final DeviceConsumer consumer, final MetricRecorder recorder) {
        this.id = id;
        this.managed = managed;
        this.publisher = publisher;
        this.consumer = consumer;
        this.recorder = recorder;

        this.handler = command -> {
            System.out.printf("Command received %s\n", command.getDevice());
            this.recorder.commandReceived();
        };
    }

    public void start() {
        doAndRecord(publisher.connect(MessageProtos.Connect.newBuilder().setDevice(id).addAllManaged(managed).build()),
                recorder::connectSucceeded, recorder::connectFailed);

        consumer.registerHandler(handler);
    }

    public void stop() {
        publisher.disconnect(MessageProtos.Disconnect.newBuilder().setDevice(id).build());
    }

    public void report() {
        doAndRecord(publisher.report(reportingMessage(id)), recorder::reportSucceeded, recorder::reportFailed);
        managed.forEach(d ->
                doAndRecord(publisher.report(reportingMessage(d)), recorder::reportSucceeded, recorder::reportFailed));
    }

    private MessageProtos.Report reportingMessage(final String id) {
        return MessageProtos.Report.newBuilder().setDevice(id).setMessage("test").build();
    }

    private void doAndRecord(final boolean predicate, final Action success, final Action failure) {
        if (predicate) {
            success.execute();
        } else {
            failure.execute();
        }
    }
}
