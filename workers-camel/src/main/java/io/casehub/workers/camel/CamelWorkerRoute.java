package io.casehub.workers.camel;

import org.apache.camel.ExchangePattern;

public interface CamelWorkerRoute {
    String capabilityTag();
    String entryUri();
    ExchangePattern exchangePattern();
}
