package com.bgch.edge.scaling.mqtt;

import com.bgch.edge.scaling.device.DeviceConsumer;
import com.bgch.edge.scaling.device.DevicePublisher;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;

public final class Device {
    private final DeviceConsumer consumer;
    private final DevicePublisher publisher;

    public Device(final String broker, final MqttConnectOptions options, final MqttClientPersistence persistence)    {
        final ReconnectingClient client = new ReconnectingClient(broker, options, persistence, MqttClient.generateClientId(), "fromPlatform");
        consumer = new MqttDeviceConsumer(client);
        publisher = new MqttDevicePublisher(client, "fromPlatform");

    }

    public final void start()   {

    }

    public final void stop()    {

    }
}
