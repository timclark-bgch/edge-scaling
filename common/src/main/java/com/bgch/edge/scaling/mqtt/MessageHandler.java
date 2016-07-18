package com.bgch.edge.scaling.mqtt;

import org.eclipse.paho.client.mqttv3.MqttMessage;

@FunctionalInterface
public interface MessageHandler  {
    void consume(String topic, MqttMessage message);
}
