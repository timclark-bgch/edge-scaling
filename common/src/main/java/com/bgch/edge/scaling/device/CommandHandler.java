package com.bgch.edge.scaling.device;

import honeycomb.messages.MessageProtos;

public interface CommandHandler {
    void handle(MessageProtos.Command command);
}
