package com.bgch.edge.scaling.platform;

import honeycomb.messages.MessageProtos;

public interface PlatformPublisher {
    void sendCommand(MessageProtos.Command command);
}
