package com.github.highcumontoa.delivery.config;

import com.github.highcumontoa.delivery.store.EventStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class StoreConfig {

    @Bean
    public EventStore eventStore(DeliveryProperties properties) {
        EventStore store = new EventStore(properties.getStorePath());
        store.recover();
        return store;
    }
}
