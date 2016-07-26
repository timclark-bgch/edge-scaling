package com.bgch.edge.scaling.mqtt;

import com.bgch.edge.scaling.device.Device;
import com.bgch.edge.scaling.device.MetricRecorder;
import com.google.common.collect.Lists;
import honeycomb.messages.MessageProtos;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public final class MqttDeviceTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Ignore
    @Test
    public void publishesAndConsumes() throws IOException, MqttException, InterruptedException {
        final MqttConnection connection = new MqttConnection(factory(), MqttClient.generateClientId());

        final String id = "device";
        final ArrayList<String> managed = Lists.newArrayList("m1", "m2", "m3");
        final MqttDevicePublisher publisher = new MqttDevicePublisher(connection, "fromDevice");
        final MqttDeviceConsumer consumer = new MqttDeviceConsumer(connection, id, managed);

        final TestRecorder recorder = new TestRecorder();
        final Device device = new Device(id, managed, publisher, consumer, recorder, 100);
        device.start();

        final MessageProtos.Command command = MessageProtos.Command.newBuilder().setDevice("test").build();
        connection.send("fromPlatform/m1", command.toByteArray(), 0, false);
        connection.send("fromPlatform/m2", command.toByteArray(), 0, false);
        connection.send("fromPlatform/m3", "test".getBytes(), 0, false);

        device.report();
        device.report();

        TimeUnit.SECONDS.sleep(1);

        device.stop();

        assertThat(recorder.connect.successCount()).isEqualTo(1);
        assertThat(recorder.connect.failCount()).isEqualTo(0);
        assertThat(recorder.report.successCount()).isEqualTo(2 * (1 + managed.size()));
        assertThat(recorder.report.failCount()).isEqualTo(0);
        assertThat(recorder.command.get()).isEqualTo(2);
    }

    private MqttConnectionFactory factory() throws IOException {
        final MqttDefaultFilePersistence persistence = new MqttDefaultFilePersistence(folder.newFolder().getAbsolutePath());
        return new MqttConnectionFactory("tcp://localhost:1883", new MqttConnectOptions(), persistence);
    }

    private class TestMetric {
        private AtomicInteger success = new AtomicInteger(0);
        private AtomicInteger failure = new AtomicInteger(0);

        void success()  {
            success.getAndIncrement();
        }

        void fail() {
            failure.getAndIncrement();
        }

        int successCount()  {
            return success.get();
        }

        int failCount() {
            return failure.get();
        }
    }

    private class TestRecorder implements MetricRecorder    {
        final TestMetric connect = new TestMetric();
        final TestMetric report = new TestMetric();
        final AtomicInteger command = new AtomicInteger(0);

        @Override
        public void connectSucceeded() {
            connect.success();
        }

        @Override
        public void connectFailed() {
            connect.fail();
        }

        @Override
        public void reportSucceeded() {
            report.success();
        }

        @Override
        public void reportFailed() {
            report.fail();
        }

        @Override
        public void commandReceived() {
            command.getAndIncrement();
        }
    }

}
