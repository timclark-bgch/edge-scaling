package com.bgch.edge.scaling.platform;

import honeycomb.messages.MessageProtos;

@FunctionalInterface
public interface ReportHandler {
    void handle(MessageProtos.Report report);
}
