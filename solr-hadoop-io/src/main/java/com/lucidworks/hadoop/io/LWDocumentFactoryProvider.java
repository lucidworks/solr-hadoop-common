package com.lucidworks.hadoop.io;

import java.io.IOException;
import java.util.ServiceLoader;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LWDocumentFactoryProvider {

  private static LWDocumentFactory documentFactory = null;
  private static final Logger log = LoggerFactory.getLogger(LWDocumentFactoryProvider.class);

  /**
   * Init Service Loader.
   */
  public static void init(JobConf conf) throws IOException {
    if (documentFactory == null) {
      final ServiceLoader<LWDocumentFactory> serviceLoader = ServiceLoader
          .load(LWDocumentFactory.class);
      for (LWDocumentFactory factory : serviceLoader) {
        log.info("Loading LWDocumentFactory class: " + factory.getClass());
        // Get the first service implementation.
        documentFactory = factory;
        if (conf != null) {
          documentFactory.init(conf);
        }
        break;
      }
    }
  }

  public static LWDocumentFactory getDocumentFactory(JobConf conf) {
    if (documentFactory != null) {
      return documentFactory;
    }
    final ServiceLoader<LWDocumentFactory> serviceLoader = ServiceLoader
        .load(LWDocumentFactory.class);
    for (LWDocumentFactory factory : serviceLoader) {
      log.debug("Loading LWDocumentFactory class on Mapper: " + factory.getClass());
      // Get the first service implementation.
      documentFactory = factory;
      if (conf != null) {
        documentFactory.init(conf);
      }
      return documentFactory;
    }

    // TODO: Thrown a readable error.
    return null;
  }

}
