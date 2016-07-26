package com.bgch.edge.scaling.runner.device;

import com.bgch.edge.scaling.amqp.AmqpDeviceConsumer;
import com.bgch.edge.scaling.amqp.AmqpDevicePublisher;
import com.bgch.edge.scaling.device.Device;
import com.bgch.edge.scaling.metrics.DeviceRecorder;
import com.google.common.collect.Lists;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class AmqpDeviceRunner {
    private final List<Device> devices = Lists.newArrayList();

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);

    private AmqpDeviceRunner(final String[] args) {
        final int runners = extractRunnerCount(args);
        final Connection connection = connection(extractBroker(args));
        final DeviceRecorder recorder = new DeviceRecorder();
        for (int i = 0; i < runners; i++) {
            try {
                devices.add(device(connection, recorder, extractMessageSize(args)));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        System.out.printf("AmqpDeviceRunner created: Running %d runners, Broker at %s, Reporting to %s\n", runners, extractBroker(args), extractReportingServer(args));
    }

    private Device device(final Connection connection, final DeviceRecorder recorder, final int messageSize) throws IOException {
        final String id = UUID.randomUUID().toString();
        final List<String> managed = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            managed.add(UUID.randomUUID().toString());
        }
        final AmqpDevicePublisher publisher = new AmqpDevicePublisher(connection);
        final AmqpDeviceConsumer consumer = new AmqpDeviceConsumer(connection, managed);
        return new Device(id, managed, publisher, consumer, recorder, messageSize);
    }

    private String extractBroker(final String[] args) {
        if (args.length > 0) {
            return args[0];
        }
        return "amqp://localhost:5672";
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

    private int extractMessageSize(final String[] args) {
        if (args.length > 3) {
            try {
                return Integer.parseInt(args[3]);
            } catch (NumberFormatException ignored) {
            }
        }

        return 100;
    }

    private Connection connection(final String broker) {
        final ConnectionFactory factory = new ConnectionFactory();

        try {
            factory.setUri(broker);
            return factory.newConnection();
        } catch (IOException | TimeoutException | NoSuchAlgorithmException | KeyManagementException | URISyntaxException e) {
            e.printStackTrace();
        }

        return null;
    }

    private void start() {
        System.out.println("Running Amqp device runner");

        devices.forEach(Device::start);
        devices.forEach(Device::report);
        final Runnable command = () -> devices.parallelStream().forEach(Device::report);
        executor.scheduleAtFixedRate(command, 1, 1, TimeUnit.SECONDS);
    }

    private void stop() {
        System.out.println("Shutting down Amqp device runner");

        executor.shutdownNow();
        devices.forEach(Device::stop);
    }

    public static void main(final String[] args) {
        final AmqpDeviceRunner runner = new AmqpDeviceRunner(args);
        runner.start();

        Runtime.getRuntime().addShutdownHook(new Thread(runner::stop));
    }
}
