package com.bgch.edge.scaling.device;

import honeycomb.messages.MessageProtos;

public interface DeviceConsumer {
    void handleCommand(MessageProtos.Command command);
}
