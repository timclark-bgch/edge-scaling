package com.bgch.edge.scaling.mqtt;

import org.eclipse.paho.client.mqttv3.MqttMessage;

@FunctionalInterface
interface MessageHandler  {
    void consume(String topic, MqttMessage message);
}
