package com.bgch.edge.scaling.runner.platform;

import com.bgch.edge.scaling.amqp.AmqpPlatformConsumer;
import com.bgch.edge.scaling.metrics.PlatformRecorder;
import com.bgch.edge.scaling.platform.Platform;
import com.google.common.collect.Lists;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

public final class AmqpPlatformRunner {
    private final List<Platform> platforms = Lists.newArrayList();
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);

    private AmqpPlatformRunner(final String[] args) {
        final int runners = extractRunnerCount(args);
        final Connection connection = connection(extractBroker(args));
        final PlatformRecorder recorder = new PlatformRecorder();

        for (int i = 0; i < runners; i++) {
            try {
                platforms.add(platform(connection, recorder));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        System.out.printf("AmqpPlatformRunner created: Running %d runners, Broker at %s, Reporting to %s\n", runners, extractBroker(args), extractReportingServer(args));
    }

    private Platform platform(final Connection connection, final PlatformRecorder recorder) throws IOException {
        return new Platform(new AmqpPlatformConsumer(connection), recorder);
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
        System.out.println("Running Amqp platform runner");

        platforms.forEach(Platform::start);
    }

    private void stop() {
        System.out.println("Shutting down Amqp device runner");

        executor.shutdownNow();
        platforms.forEach(Platform::stop);
    }

    public static void main(final String[] args) {
        final AmqpPlatformRunner runner = new AmqpPlatformRunner(args);
        runner.start();

        Runtime.getRuntime().addShutdownHook(new Thread(runner::stop));
    }

}
