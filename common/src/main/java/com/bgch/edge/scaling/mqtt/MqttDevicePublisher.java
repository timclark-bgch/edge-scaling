package com.bgch.edge.scaling.mqtt;

import com.bgch.edge.scaling.device.DevicePublisher;
import honeycomb.messages.MessageProtos;

final class MqttDevicePublisher implements DevicePublisher {
    private final ReconnectingClient client;
    private final String topic;

    MqttDevicePublisher(final ReconnectingClient client, final String topic) {
        this.client = client;
        this.topic = topic;
    }

    @Override
    public boolean connect(final MessageProtos.Connect connect) {
        return client.send(Topics.CONNECTED, connect.toByteArray(), 0, false);
    }

    @Override
    public boolean disconnect(final MessageProtos.Disconnect disconnect) {
        return client.send(Topics.DISCONNECTED, disconnect.toByteArray(), 0, false);
    }

    @Override
    public boolean report(final MessageProtos.Report report) {
        return client.send(topic, report.toByteArray(), 0, false);
    }
}
