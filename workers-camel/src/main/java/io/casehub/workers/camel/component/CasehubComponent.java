package io.casehub.workers.camel.component;

import org.apache.camel.Endpoint;
import org.apache.camel.support.DefaultComponent;
import java.util.Map;

public class CasehubComponent extends DefaultComponent {
    @Override
    protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) {
        return new CasehubEndpoint(uri, this);
    }
}
