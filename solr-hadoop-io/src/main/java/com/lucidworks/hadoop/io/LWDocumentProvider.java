package com.lucidworks.hadoop.io;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.lucidworks.hadoop.LWDocumentModule;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LWDocumentProvider {

  private static final Logger log = LoggerFactory.getLogger(LWDocumentProvider.class);

  private static Injector injector = Guice.createInjector(new LWDocumentModule());

  public static void configure(JobConf conf) throws IOException {
    // XXX: Static? Guice
    injector.getInstance(LWDocument.class).configure(conf);
  }

  public static void init(JobConf conf) throws IOException {
    // XXX: Static? Guice
    injector.getInstance(LWDocument.class).init(conf);
  }

  public static LWDocument createDocument() {
    return injector.getInstance(LWDocument.class);
  }

  public static LWDocument createDocument(String id, Map<String, String> metadata) {
    LWDocument document = injector.getInstance(LWDocument.class);
    document.setId(id);
    document.setMetadata(metadata);
    return document;
  }

}
