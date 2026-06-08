package io.casehub.workers.camel.component;

import io.casehub.workers.camel.CamelWorkerFaultPublisher;
import io.casehub.workers.common.AsyncWorkerCompletionRegistry;
import io.casehub.workers.common.WorkflowCompletionPublisher;
import jakarta.enterprise.inject.spi.CDI;
import org.apache.camel.Consumer;
import org.apache.camel.Producer;
import org.apache.camel.support.DefaultEndpoint;

public class CasehubEndpoint extends DefaultEndpoint {

    public CasehubEndpoint(String endpointUri, CasehubComponent component) {
        super(endpointUri, component);
    }

    @Override
    public Producer createProducer() {
        return new CasehubProducer(this,
            CDI.current().select(AsyncWorkerCompletionRegistry.class).get(),
            CDI.current().select(WorkflowCompletionPublisher.class).get(),
            CDI.current().select(CamelWorkerFaultPublisher.class).get());
    }

    @Override
    public Consumer createConsumer(org.apache.camel.Processor processor) {
        throw new UnsupportedOperationException("casehub:complete is producer-only");
    }

    @Override
    public boolean isSingleton() {
        return true;
    }
}
