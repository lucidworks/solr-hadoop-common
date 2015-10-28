package com.lucidworks.hadoop.io.impl;

import com.lucidworks.hadoop.io.LWDocument;
import com.lucidworks.hadoop.io.LWDocumentFactory;
import java.util.Map;
import org.apache.hadoop.mapred.JobConf;

public class LWSolrDocumentFactory implements LWDocumentFactory {
  @Override
  public LWDocument createDocument() {
    return new LWSolrDocument();
  }

  @Override
  public LWDocument createDocument(String id, Map<String, String> metadata) {
    return new LWSolrDocument(id, metadata);
  }

  @Override
  public void configure(JobConf conf) {

  }

  @Override
  public void init(JobConf conf) {
    // do nothing
  }
}
