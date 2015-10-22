package com.lucidworks.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import org.apache.solr.common.SolrInputDocument;

public interface LWDocument {

  public LWDocument[] process();

  public void setId(String id);

  public String getId();

  public Map<String, String> getMetadata();

  public void write(DataOutput dataOutput) throws IOException;

  public void readFields(DataInput dataInput) throws IOException;

  public void setContent(byte[] data);

  public LWDocument addField(String name, Object value);

  public LWDocument addMetadata(String name, String value);

  /* Check if the document has multiple id fields, use one of them or the missingId argument., */
  public LWDocument checkId(String idField, String missingId);

  public Object getFirstFieldValue(String name);

  public SolrInputDocument convertToSolr();

}
