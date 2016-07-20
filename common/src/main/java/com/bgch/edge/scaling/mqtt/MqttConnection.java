package com.bgch.edge.scaling.mqtt;

import com.github.rholder.retry.*;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.eclipse.paho.client.mqttv3.*;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public final class MqttConnection {
    private final MqttConnectionFactory factory;
    private final String id;

    private final List<MessageHandler> handlers = Lists.newArrayList();
    private Set<String> topics = Sets.newHashSet();

    public MqttConnection(final MqttConnectionFactory factory, final String id) throws MqttException {
        this.factory = factory;
        this.id = id;

        connect();
    }

    private MqttAsyncClient client;

    void addHandler(final MessageHandler handler, final List<String> filters) {
        topics.addAll(filters);
        topics.forEach(this::subscribe);

        handlers.add(handler);
    }

    private void subscribe(final String topic) {
        try {
            client.subscribe(topic, 0);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    boolean connect() {
        try {
            client = factory.client(id);
            client.setCallback(callback);
            topics.forEach(this::subscribe);

            return true;
        } catch (MqttException e) {
            e.printStackTrace();
            return false;
        }
    }

    void disconnect() {
        try {
            client.disconnect();
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    boolean send(final String topic, final byte[] payload, final int qos, final boolean retain) {
        try {
            client.publish(topic, payload, qos, retain);
            return true;
        } catch (MqttException e) {

            e.printStackTrace();
            return false;
        }
    }

    private final MqttCallback callback = new MqttCallback() {
        @Override
        public void connectionLost(final Throwable throwable) {
            final Retryer<Boolean> retryer = RetryerBuilder.<Boolean>newBuilder()
                    .retryIfResult(v -> v != null && !v)
                    .withWaitStrategy(WaitStrategies.exponentialWait())
                    .withStopStrategy(StopStrategies.stopAfterDelay(30, TimeUnit.SECONDS))
                    .build();

            try {
                retryer.call(() -> connect());
            } catch (ExecutionException | RetryException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void messageArrived(final String topic, final MqttMessage message) throws Exception {
            handlers.forEach(h -> h.consume(topic, message));
        }

        @Override
        public void deliveryComplete(final IMqttDeliveryToken iMqttDeliveryToken) {

        }
    };


}
