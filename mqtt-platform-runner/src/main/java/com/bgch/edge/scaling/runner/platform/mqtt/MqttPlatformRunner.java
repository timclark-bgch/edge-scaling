package com.bgch.edge.scaling.runner.platform.mqtt;

import com.bgch.edge.scaling.metrics.PlatformRecorder;
import com.bgch.edge.scaling.mqtt.MqttConnection;
import com.bgch.edge.scaling.mqtt.MqttConnectionFactory;
import com.bgch.edge.scaling.mqtt.MqttPlatformConsumer;
import com.bgch.edge.scaling.platform.Platform;
import com.google.common.collect.Lists;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;

import java.util.List;
import java.util.stream.Collectors;

public final class MqttPlatformRunner {

    public static void main(final String[] args) throws MqttException {
        System.out.println("Running Mqtt platform runner");
        final MqttConnectionFactory connectionFactory = new MqttConnectionFactory("tcp://localhost:1883", new MqttConnectOptions(), new MqttDefaultFilePersistence("/tmp/mqtt"));
        final PlatformRecorder platformRecorder = new PlatformRecorder();

        final List<MqttConnection> connections = Lists.newArrayList();
        for (int i = 0; i < 1; i++) {
            connections.add(new MqttConnection(connectionFactory, MqttClient.generateClientId()));
        }

        final List<Platform> platforms = connections.stream().map(c -> platform(c, platformRecorder)).collect(Collectors.toList());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down Mqtt platform runner");
            platforms.forEach(Platform::stop);
            connections.forEach(MqttConnection::disconnect);
        }));
    }

    private static Platform platform(final MqttConnection connection, final PlatformRecorder recorder) {
        return new Platform(new MqttPlatformConsumer(connection, "fromDevice"), recorder);
    }
}
