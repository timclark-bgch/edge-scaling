package com.bgch.edge.scaling.platform;

import honeycomb.messages.MessageProtos;

@FunctionalInterface
public interface DisconnectHandler {
    void handle(MessageProtos.Disconnect disconnect);
}
