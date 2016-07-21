package com.bgch.edge.scaling.runner.device;

import com.bgch.edge.scaling.device.Device;
import com.bgch.edge.scaling.metrics.DeviceRecorder;
import com.bgch.edge.scaling.mqtt.MqttConnection;
import com.bgch.edge.scaling.mqtt.MqttConnectionFactory;
import com.bgch.edge.scaling.mqtt.MqttDeviceConsumer;
import com.bgch.edge.scaling.mqtt.MqttDevicePublisher;
import com.google.common.collect.Lists;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public final class MqttDeviceRunner {
    private final int runners;
    private final MqttConnectionFactory factory;
    private final List<MqttConnection> connections = Lists.newArrayList();
    private final List<Device> devices = Lists.newArrayList();
    private final DeviceRecorder recorder;

    private MqttDeviceRunner(final String[] args)   {
        this.factory = new MqttConnectionFactory(extractBroker(args), new MqttConnectOptions(), new MqttDefaultFilePersistence("/tmp/mqtt"));
        this.runners = extractRunnerCount(args);
        this.recorder = new DeviceRecorder();

        System.out.printf("MqttDeviceRunner created: Running %d runners, Broker at %s, Reporting to %s\n", runners, extractBroker(args), extractReportingServer(args));
    }

    private void start() throws MqttException {
        System.out.println("Running Mqtt device runner");
        for (int i = 0; i < runners; i++) {
            connections.add(new MqttConnection(factory, MqttClient.generateClientId()));
        }

        devices.addAll(connections.stream().map(c -> device(c, recorder)).collect(Collectors.toList()));
        devices.forEach(Device::start);
        devices.forEach(Device::report);
    }

    private Device device(final MqttConnection connection, final DeviceRecorder recorder)   {
        final String id = UUID.randomUUID().toString();
        final List<String> managed = Lists.newArrayList();
        for(int i = 0; i < 10; i++) {
            managed.add(UUID.randomUUID().toString());
        }

        final MqttDevicePublisher publisher = new MqttDevicePublisher(connection, "fromDevice");
        final MqttDeviceConsumer consumer = new MqttDeviceConsumer(connection, id, managed);
        return new Device(id, managed, publisher, consumer, recorder);
    }

    private void stop() {
        System.out.println("Shutting down Mqtt device runner");
        devices.forEach(Device::stop);
        connections.forEach(MqttConnection::disconnect);
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

    public static void main(final String[] args) throws MqttException {
        final MqttDeviceRunner runner = new MqttDeviceRunner(args);
        runner.start();

        Runtime.getRuntime().addShutdownHook(new Thread(runner::stop));
    }
}
