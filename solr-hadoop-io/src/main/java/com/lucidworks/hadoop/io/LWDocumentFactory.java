package com.lucidworks.hadoop.io;

import java.util.Map;
import org.apache.hadoop.mapred.JobConf;

public interface LWDocumentFactory {

  public LWDocument createDocument();

  public LWDocument createDocument(String id, Map<String, String> metadata);

  public void init(JobConf conf);

}
