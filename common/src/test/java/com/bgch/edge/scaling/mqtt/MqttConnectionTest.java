package com.bgch.edge.scaling.mqtt;

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

public class MqttConnectionTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();


    @Test
    public void useConnection() throws IOException, MqttException, InterruptedException {
        final MqttConnection connection = new MqttConnection(factory(), MqttClient.generateClientId());

        final AtomicInteger received = new AtomicInteger(0);
        connection.addHandler((t, m) -> received.getAndIncrement(), "test");
        connection.send("test", "test".getBytes(), 0, false);
        connection.send("test", "test".getBytes(), 0, false);
        connection.send("test", "test".getBytes(), 0, false);

        TimeUnit.SECONDS.sleep(1);

        connection.disconnect();

        assertThat(received.get()).isEqualTo(3);
    }

    private MqttConnectionFactory factory() throws IOException {
        final MqttDefaultFilePersistence persistence = new MqttDefaultFilePersistence(folder.newFolder().getAbsolutePath());
        return new MqttConnectionFactory("tcp://localhost:1883", new MqttConnectOptions(), persistence);
    }
}