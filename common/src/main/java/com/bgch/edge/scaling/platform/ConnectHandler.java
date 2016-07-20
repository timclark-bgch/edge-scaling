package com.bgch.edge.scaling.platform;

import honeycomb.messages.MessageProtos;

@FunctionalInterface
public interface ConnectHandler {
    void handle(MessageProtos.Connect connect);
}
