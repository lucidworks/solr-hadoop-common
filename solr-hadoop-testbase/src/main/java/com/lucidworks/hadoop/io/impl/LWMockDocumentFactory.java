package com.lucidworks.hadoop.io.impl;

import com.lucidworks.hadoop.io.LWDocument;
import com.lucidworks.hadoop.io.LWDocumentFactory;
import java.util.Map;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LWMockDocumentFactory implements LWDocumentFactory {

  private transient static Logger log = LoggerFactory.getLogger(LWMockDocumentFactory.class);
  private JobConf conf = null;

  @Override
  public LWDocument createDocument() {
    return new LWMockDocument();
  }

  @Override
  public LWDocument createDocument(String id, Map<String, String> metadata) {
    return new LWMockDocument(id, metadata);
  }

  @Override
  public void configure(JobConf conf) {

  }

  @Override
  public void init(JobConf conf) {
    this.conf = conf;
  }
}
