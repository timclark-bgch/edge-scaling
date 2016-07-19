package com.bgch.edge.scaling.mqtt;

import com.github.rholder.retry.*;
import com.google.common.collect.Lists;
import org.eclipse.paho.client.mqttv3.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

final class ReconnectingClient {

    private final String broker;
    private final String id;
    private final MqttConnectOptions options;
    private final MqttClientPersistence persistence;
    private final List<String> topics;
    private final ArrayList<MessageHandler> handlers = Lists.newArrayList();

    private final MqttCallback callback = new MqttCallback() {
        @Override
        public void connectionLost(final Throwable throwable) {
            final Retryer<Void> retry = RetryerBuilder.<Void>newBuilder()
                    .retryIfException()
                    .withWaitStrategy(WaitStrategies.exponentialWait())
                    .withStopStrategy(StopStrategies.stopAfterDelay(30, TimeUnit.SECONDS))
                    .build();

            try {
                retry.call(() -> {
                    connect();
                    return null;
                });
            } catch (ExecutionException | RetryException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void messageArrived(final String topic, final MqttMessage mqttMessage) throws Exception {
            handlers.forEach(h -> h.consume(topic, mqttMessage));
        }

        @Override
        public void deliveryComplete(final IMqttDeliveryToken iMqttDeliveryToken) {
        }
    };

    private MqttAsyncClient client;

    ReconnectingClient(final String broker, final MqttConnectOptions options, final MqttClientPersistence persistence, final String id, final String... topics) {
        this.options = options;
        this.broker = broker;
        this.id = id;
        this.topics = Arrays.asList(topics);
        this.persistence = persistence;
    }

    void addHandler(final MessageHandler handler)   {
        handlers.add(handler);
    }

    void connect() throws MqttException {
        client = new MqttAsyncClient(broker, id, persistence);
        client.connect(options).waitForCompletion();
        client.setCallback(callback);
        topics.forEach(this::subscribe);
    }

    private void subscribe(final String topic) {
        try {
            client.subscribe(topic, 0);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    boolean send(final String topic, final byte[] payload, final int qos, final boolean retain) {
//        try {
//            client.publish(topic, payload, qos, retain);
//            return true;
//        } catch (MqttException e) {
//
//            e.printStackTrace();
//            return false;
//        }
        return false;
    }

    void disconnect() {
        try {
            client.disconnect();
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }
}