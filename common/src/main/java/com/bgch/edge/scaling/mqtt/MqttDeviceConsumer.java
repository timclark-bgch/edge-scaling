package com.bgch.edge.scaling.mqtt;

import com.bgch.edge.scaling.device.CommandHandler;
import com.bgch.edge.scaling.device.DeviceConsumer;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import honeycomb.messages.MessageProtos;

import java.util.List;
import java.util.stream.Collectors;

public final class MqttDeviceConsumer implements DeviceConsumer {
    private final List<CommandHandler> handlers = Lists.newArrayList();

    public MqttDeviceConsumer(final MqttConnection connection, final String id, final List<String> managed) {
        final MessageHandler handler = (topic, message) -> {
            System.out.printf("MSG %s %d\n", topic, message.getPayload().length);
            handlers.forEach(h -> {
                try {
                    h.handle(MessageProtos.Command.parseFrom(message.getPayload()));
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                }
            });

        };
        connection.addHandler(handler, topics(id, managed));
    }

    @Override
    public void registerHandler(final CommandHandler handler) {
        handlers.add(handler);
    }

    private List<String> topics(final String id, final List<String> managed) {
        final List<String> topics = managed.stream()
                .map(this::topicName)
                .collect(Collectors.toList());

        topics.add(id);
        return topics;
    }

    private String topicName(final String topic) {
        return String.format("fromPlatform/%s", topic);
    }

}
