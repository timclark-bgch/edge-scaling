package com.bgch.edge.scaling.mqtt;

import com.bgch.edge.scaling.device.DevicePublisher;
import honeycomb.messages.MessageProtos;

public final class MqttDevicePublisher implements DevicePublisher {
    private final MqttConnection connection;
    private final String reportingTopic;

    public MqttDevicePublisher(final MqttConnection connection, final String reportingTopic) {
        this.connection = connection;
        this.reportingTopic = reportingTopic;
    }

    @Override
    public boolean connect(final MessageProtos.Connect connect) {
        return connection.send(Topics.CONNECTED, connect.toByteArray(), 0, false);
    }

    @Override
    public boolean disconnect(final MessageProtos.Disconnect disconnect) {
        return connection.send(Topics.DISCONNECTED, disconnect.toByteArray(), 0, false);
    }

    @Override
    public boolean report(final MessageProtos.Report report) {
        return connection.send(reportingTopic, report.toByteArray(), 0, false);
    }
}
