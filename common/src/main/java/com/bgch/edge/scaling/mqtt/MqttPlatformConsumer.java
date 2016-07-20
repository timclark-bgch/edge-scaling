package com.bgch.edge.scaling.mqtt;

import com.bgch.edge.scaling.platform.ConnectHandler;
import com.bgch.edge.scaling.platform.DisconnectHandler;
import com.bgch.edge.scaling.platform.PlatformConsumer;
import com.bgch.edge.scaling.platform.ReportHandler;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import honeycomb.messages.MessageProtos;

import java.util.List;
import java.util.Map;

final class MqttPlatformConsumer implements PlatformConsumer {
    private final MqttConnection connection;
    private final List<ConnectHandler> connectHandlers = Lists.newArrayList();
    private final List<DisconnectHandler> disconnectHandlers = Lists.newArrayList();
    private final List<ReportHandler> reportHandlers = Lists.newArrayList();
    private final Map<String, MessageHandler> messageHandlers = Maps.newHashMap();

    MqttPlatformConsumer(final MqttConnection connection, final String deviceTopic) {
        this.connection = connection;

        messageHandlers.put(Topics.CONNECTED, (topic, message) -> {
            try {
                final MessageProtos.Connect connect = MessageProtos.Connect.parseFrom(message.getPayload());
                connectHandlers.forEach(h -> h.handle(connect));
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
        });
        messageHandlers.put(Topics.DISCONNECTED, (topic, message) -> {
            try {
                final MessageProtos.Disconnect disconnect = MessageProtos.Disconnect.parseFrom(message.getPayload());
                disconnectHandlers.forEach(h -> h.handle(disconnect));
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
        });
        messageHandlers.put(deviceTopic, (topic, message) -> {
            try {
                final MessageProtos.Report report = MessageProtos.Report.parseFrom(message.getPayload());
                reportHandlers.forEach(h -> h.handle(report));
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
        });

        final MessageHandler handler = (topic, message) -> {
            final MessageHandler messageHandler = messageHandlers.get(topic);
            if (messageHandler != null) {
                messageHandler.consume(topic, message);
            }
        };

        this.connection.addHandler(handler, Lists.newArrayList(Topics.CONNECTED, Topics.DISCONNECTED, deviceTopic));
    }

    @Override
    public void registerHandler(final ConnectHandler handler) {
        connectHandlers.add(handler);
    }

    @Override
    public void registerHandler(final DisconnectHandler handler) {
        disconnectHandlers.add(handler);
    }

    @Override
    public void registerHandler(final ReportHandler handler) {
        reportHandlers.add(handler);
    }
}
