/*
 * Copyright 2020 Rackspace US, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rackspace.salus.event.processor.config;

import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.jsr107.Eh107Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.cache.JCacheManagerCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(CacheProperties.class)
@EnableCaching
public class CacheConfig {

  public static final String EXPECTED_EVENT_COUNTS = "event-counts";
  public static final String MONITOR_INTERVALS = "monitor-intervals";
  public static final String STATE_HISTORY = "state-history";

  private final CacheProperties properties;

  @Autowired
  public CacheConfig(CacheProperties properties) {
    this.properties = properties;
  }

  @Bean
  public JCacheManagerCustomizer eventProcessorCacheCustomizer() {
    return cacheManager -> {
      cacheManager.createCache(EXPECTED_EVENT_COUNTS, repositoryCacheConfig());
      cacheManager.createCache(MONITOR_INTERVALS, repositoryCacheConfig());
      cacheManager.createCache(STATE_HISTORY, repositoryCacheConfig());
    };
  }

  private javax.cache.configuration.Configuration<Object, Object> repositoryCacheConfig() {
    return Eh107Configuration.fromEhcacheCacheConfiguration(
        CacheConfigurationBuilder.newCacheConfigurationBuilder(Object.class, Object.class,
            ResourcePoolsBuilder.heap(properties.getMaxSize())
        )
    );
  }
}

