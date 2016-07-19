package com.bgch.edge.scaling.mqtt;

import honeycomb.messages.MessageProtos;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class MqttDevicePublisherTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void checkPublishing() throws IOException, MqttException {
        final MqttDefaultFilePersistence persistence = new MqttDefaultFilePersistence(folder.newFolder().getAbsolutePath());

        final String broker = "tcp://localhost:1883";
        final ReconnectingClient deviceClient = client(broker, persistence);
        deviceClient.connect();
        final MqttDevicePublisher publisher = new MqttDevicePublisher(deviceClient, "fromDevice");

        final ReconnectingClient consumerClient = client(broker, persistence, Topics.CONNECTED, Topics.DISCONNECTED, "fromDevice");
        consumerClient.addHandler((topic, message) -> System.out.printf("%s: %s\n", topic, message.getPayload().length));
        consumerClient.connect();

        publisher.connect(MessageProtos.Connect.newBuilder().setDevice("test").addManaged("child1").build());
        publisher.report(MessageProtos.Report.newBuilder().setDevice("test").setMessage("test1").build());
        publisher.report(MessageProtos.Report.newBuilder().setDevice("test").setMessage("test2").build());
        publisher.report(MessageProtos.Report.newBuilder().setDevice("test").setMessage("test3").build());
        publisher.report(MessageProtos.Report.newBuilder().setDevice("test").setMessage("test4").build());

        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        publisher.disconnect(MessageProtos.Disconnect.newBuilder().setDevice("test").build());

        deviceClient.disconnect();
        consumerClient.disconnect();
    }

    private ReconnectingClient client(final String broker, final MqttClientPersistence persistence, final String... topics) {
        return new ReconnectingClient(broker, new MqttConnectOptions(), persistence, MqttClient.generateClientId(), topics);
    }

}