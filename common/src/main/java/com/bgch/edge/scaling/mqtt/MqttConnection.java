package com.bgch.edge.scaling.mqtt;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.eclipse.paho.client.mqttv3.*;

import java.util.List;
import java.util.Set;
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
        } catch (final MqttException ignored) {
        }
    }

    private boolean connect() {
        try {
            client = factory.client(id);
            client.setCallback(callback);
            topics.forEach(this::subscribe);

            return true;
        } catch (final MqttException e) {
            recordError(e);
            return false;
        }
    }

    public void disconnect() {
        try {
            client.disconnect();
        } catch (final MqttException e) {
            recordError(e);
        }
    }

    boolean send(final String topic, final byte[] payload, final int qos, final boolean retain) {
        try {
            client.publish(topic, payload, qos, retain);
            return true;
        } catch (final MqttException e) {
            return false;
        }
    }

    private final MqttCallback callback = new MqttCallback() {
        @Override
        public void connectionLost(final Throwable throwable) {
            final RetryPolicy retryPolicy = new RetryPolicy()
                    .retryWhen(false)
                    .withBackoff(1, 30, TimeUnit.SECONDS)
                    .withJitter(0.2)
                    .withMaxDuration(30, TimeUnit.SECONDS);


            Failsafe.with(retryPolicy).get(() -> connect());
        }

        @Override
        public void messageArrived(final String topic, final MqttMessage message) throws Exception {
            handlers.forEach(h -> h.consume(topic, message));
        }

        @Override
        public void deliveryComplete(final IMqttDeliveryToken iMqttDeliveryToken) {

        }
    };

    private void recordError(final Exception e) {
        System.err.println(e.getMessage());
    }
}
