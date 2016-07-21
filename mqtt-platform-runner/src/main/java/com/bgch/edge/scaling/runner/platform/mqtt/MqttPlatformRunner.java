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
    private final int runners;
    private final MqttConnectionFactory factory;
    private final PlatformRecorder recorder;
    private final List<MqttConnection> connections = Lists.newArrayList();
    private final List<Platform> platforms  = Lists.newArrayList();

    private MqttPlatformRunner(final String[] args) {
        runners = extractRunnerCount(args);
        this.factory = new MqttConnectionFactory(extractBroker(args), new MqttConnectOptions(), new MqttDefaultFilePersistence("/tmp/mqtt"));
        this.recorder = new PlatformRecorder();

        System.out.printf("MqttPlatformRunner created: Running %d runners, Broker at %s, Reporting to %s\n", runners, extractBroker(args), extractReportingServer(args));
    }

    private String extractBroker(final String[] args) {
        if (args.length > 0) {
            return args[0];
        }
        return "tcp://localhost:1883";
    }

    private int extractRunnerCount(final String[] args) {
        if (args.length > 1) {
            try {
                return Integer.parseInt(args[1]);
            } catch (NumberFormatException ignored) {
            }
        }
        return 1;
    }

    private String extractReportingServer(final String[] args) {
        if (args.length > 2) {
            return args[2];
        }
        return "unknown";
    }

    private void start() throws MqttException {
        System.out.println("Running Mqtt platform runner");
        for (int i = 0; i < runners; i++) {
            connections.add(new MqttConnection(factory, MqttClient.generateClientId()));
        }

        platforms.addAll(connections.stream().map(c -> platform(c, recorder)).collect(Collectors.toList()));
    }

    private void stop() {
        System.out.println("Shutting down Mqtt platform runner");
        platforms.forEach(Platform::stop);
        connections.forEach(MqttConnection::disconnect);
    }

    public static void main(final String[] args) throws MqttException {
        final MqttPlatformRunner runner = new MqttPlatformRunner(args);
        runner.start();

        Runtime.getRuntime().addShutdownHook(new Thread(runner::stop));
    }

    private Platform platform(final MqttConnection connection, final PlatformRecorder recorder) {
        return new Platform(new MqttPlatformConsumer(connection, "fromDevice"), recorder);
    }
}
