package com.bgch.edge.scaling.mqtt;

import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;

public final class MqttConnectionFactory {
    private final String broker;
    private final MqttConnectOptions options;
    private final MqttClientPersistence persistence;

    public MqttConnectionFactory(final String broker, final MqttConnectOptions options, final MqttClientPersistence persistence) {
        this.broker = broker;
        this.options = options;
        this.persistence = persistence;
    }

    MqttAsyncClient client(final String id) throws MqttException {
        final MqttAsyncClient client = new MqttAsyncClient(broker, id, persistence);
        client.connect(options).waitForCompletion();

        return client;
    }
}
