package com.bgch.edge.scaling.mqtt;

import com.bgch.edge.scaling.device.DeviceConsumer;
import com.google.protobuf.InvalidProtocolBufferException;
import honeycomb.messages.MessageProtos;

final class MqttDeviceConsumer implements DeviceConsumer {
    private final ReconnectingClient client;

    MqttDeviceConsumer(final ReconnectingClient client) {
        this.client = client;
        client.addHandler((topic, message) -> {
            try {
                handleCommand(MessageProtos.Command.parseFrom(message.getPayload()));
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void handleCommand(final MessageProtos.Command command) {
        // TODO record metric for command.
    }
}
