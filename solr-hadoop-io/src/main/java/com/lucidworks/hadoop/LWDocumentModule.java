package com.lucidworks.hadoop;

import com.google.inject.AbstractModule;
import com.lucidworks.hadoop.io.LWDocument;
import java.util.ServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LWDocumentModule extends AbstractModule {

  private static final Logger log = LoggerFactory.getLogger(LWDocumentModule.class);

  @Override
  protected void configure() {

    final ServiceLoader<LWDocument> serviceLoader = ServiceLoader.load(LWDocument.class);
    for (LWDocument document : serviceLoader) {
      log.info("Loading LWDocument class: " + document.getClass());
      // Get the first service implementation.
      bind(LWDocument.class).to(document.getClass());
      break;
    }
  }
}

