package com.bgch.edge.scaling.mqtt;

import com.bgch.edge.scaling.device.DevicePublisher;
import honeycomb.messages.MessageProtos;
import org.eclipse.paho.client.mqttv3.MqttException;

final class MqttDevicePublisher implements DevicePublisher {
    private final ReconnectingClient client;
    private final String topic;

    MqttDevicePublisher(final ReconnectingClient client, final String topic) {
        this.client = client;
        this.topic = topic;
    }

    @Override
    public boolean connect(final MessageProtos.Connect connect) {
        try {
            client.connect();
            return client.send(Topics.CONNECTED, connect.toByteArray(), 0, false);
        } catch (MqttException e) {
            e.printStackTrace();
        }

        return false;
    }

    @Override
    public boolean disconnect(final MessageProtos.Disconnect disconnect) {
        final boolean result = client.send(Topics.DISCONNECTED, disconnect.toByteArray(), 0, false);
        client.disconnect();

        return result;
    }

    @Override
    public boolean report(final MessageProtos.Report report) {
        // TODO record metric for report
        return client.send(topic, report.toByteArray(), 0, false);
    }
}
