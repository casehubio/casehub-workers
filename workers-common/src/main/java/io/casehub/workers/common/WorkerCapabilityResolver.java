package io.casehub.workers.common;

import java.util.Optional;
import java.util.Set;

public interface WorkerCapabilityResolver<T> {
    T resolve(String capabilityTag);
    Optional<String> firstMatch(Set<String> capabilities);
    Set<String> capabilities();
}
