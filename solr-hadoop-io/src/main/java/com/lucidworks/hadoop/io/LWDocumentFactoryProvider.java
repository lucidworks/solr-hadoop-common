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
  public static void init(JobConf conf) {
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
        // Get the first service
        break;
      }
    }
  }

  public static void configure(JobConf conf) throws IOException {
    if (documentFactory == null) {
      log.debug("Try to load the LWDocumentFactory one more time");
      // Try to load the LWDocumentFactory one more time
      LWDocumentFactoryProvider.init(conf);
    }
    if (documentFactory != null) {
      if (conf != null) {
        documentFactory.configure(conf);
      }
    } else {
      throw new IOException("LWDocumentFactory Service not found.");
    }
  }

  public static LWDocumentFactory getDocumentFactory()  {
      return documentFactory;
  }

}
