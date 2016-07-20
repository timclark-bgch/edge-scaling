package com.bgch.edge.scaling.mqtt;

import com.bgch.edge.scaling.platform.MetricRecorder;
import com.bgch.edge.scaling.platform.Platform;
import honeycomb.messages.MessageProtos;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public final class MqttPlatformTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void consumesMessages() throws IOException, MqttException, InterruptedException {
        final MqttConnection connection = new MqttConnection(factory(), MqttClient.generateClientId());

        final String deviceTopic = "fromDevice";
        final MqttPlatformConsumer platformConsumer = new MqttPlatformConsumer(connection, deviceTopic);

        final TestRecorder recorder = new TestRecorder();
        final Platform platform = new Platform(platformConsumer, recorder);

        platform.start();

        final MessageProtos.Report report = MessageProtos.Report.newBuilder().setDevice("device").setMessage("test").build();
        connection.send(deviceTopic, report.toByteArray(), 0, false);
        connection.send(deviceTopic, report.toByteArray(), 0, false);
        connection.send(deviceTopic, "test".getBytes(), 0, false);

        TimeUnit.SECONDS.sleep(1);

        platform.stop();

        assertThat(recorder.connections.get()).isEqualTo(0);
        assertThat(recorder.disconnections.get()).isEqualTo(0);
        assertThat(recorder.reports.get()).isEqualTo(2);
    }

    private MqttConnectionFactory factory() throws IOException {
        final MqttDefaultFilePersistence persistence = new MqttDefaultFilePersistence(folder.newFolder().getAbsolutePath());
        return new MqttConnectionFactory("tcp://localhost:1883", new MqttConnectOptions(), persistence);
    }

    private class TestRecorder implements MetricRecorder {
        final AtomicInteger connections = new AtomicInteger(0);
        final AtomicInteger disconnections = new AtomicInteger(0);
        final AtomicInteger reports = new AtomicInteger(0);

        @Override
        public void connect() {
            connections.getAndIncrement();
        }

        @Override
        public void disconnect() {
            disconnections.getAndIncrement();
        }

        @Override
        public void report() {
            reports.getAndIncrement();
        }
    }
}
