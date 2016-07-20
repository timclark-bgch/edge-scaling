package com.bgch.edge.scaling.mqtt;

import com.bgch.edge.scaling.device.Device;
import com.google.common.collect.Lists;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public final class MqttDeviceTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void publishesAndConsumes() throws IOException, MqttException, InterruptedException {
        final MqttConnection connection = new MqttConnection(factory(), MqttClient.generateClientId());

        final String id = "device";
        final ArrayList<String> managed = Lists.newArrayList("m1", "m2", "m3");
        final MqttDevicePublisher publisher = new MqttDevicePublisher(connection, "fromDevice");
        final MqttDeviceConsumer consumer = new MqttDeviceConsumer(connection, id, managed);

        final Device device = new Device(id, managed, publisher, consumer);
        device.start();

        connection.send("fromPlatform/m1", "test".getBytes(), 0, false);
        connection.send("fromPlatform/m2", "test".getBytes(), 0, false);
        connection.send("fromPlatform/m3", "test".getBytes(), 0, false);

        device.report();
        device.report();

        TimeUnit.SECONDS.sleep(5);

        device.stop();
    }

    private MqttConnectionFactory factory() throws IOException {
        final MqttDefaultFilePersistence persistence = new MqttDefaultFilePersistence(folder.newFolder().getAbsolutePath());
        return new MqttConnectionFactory("tcp://localhost:1883", new MqttConnectOptions(), persistence);
    }

}
