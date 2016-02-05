package com.lucidworks.hadoop.io.impl;

import com.lucidworks.hadoop.io.LWDocument;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LWMockDocument implements LWDocument {

  private transient static Logger log = LoggerFactory.getLogger(LWMockDocument.class);
  public static final String ID = "id";
  public static final String CONTENT = "content";

  private Map<String, String> fields;
  private String id;

  public LWMockDocument() {
    fields = new HashMap<>();
  }

  public LWMockDocument(String id, Map<String, String> metadata) {
    // TODO
    if (metadata != null) {
      fields = new HashMap<>(metadata);
    } else {
      fields = new HashMap<>();
    }
    this.id = id;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public void setMetadata(Map<String, String> metadata) {
    // TODO
  }

  @Override
  public LWDocument[] process() {
    LWMockDocument tmp = new LWMockDocument(id, fields);
    return new LWDocument[] { tmp };
  }

  @Override
  public void configure(JobConf conf) {

  }

  @Override
  public void init(JobConf conf) {

  }

  @Override
  public void setId(String id) {
    this.id = id;
  }

  @Override
  public Map<String, String> getMetadata() {
    //TODO create metadata
    return null;
  }

  public Map<String, String> getFields() {
    return fields;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    writeString(id, dataOutput);
    writeDocFields(fields, dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    String id = readString(dataInput);
    this.id = id;
    fields = new HashMap<>();
    readDocFields(dataInput);
  }

  private String readString(DataInput in) throws IOException {
    return Text.readString(in);
  }

  private void writeDocFields(Map<String, String> fields, DataOutput dataOutput)
      throws IOException {
    if (fields != null && fields.isEmpty() == false) {
      dataOutput.writeInt(fields.size());
      for (Map.Entry<String, String> entry : fields.entrySet()) {
        writeString(entry.getKey(), dataOutput);
        writeString(entry.getValue(), dataOutput);
      }
    } else {
      dataOutput.writeInt(0);
    }
  }

  private void writeString(String str, DataOutput dataOutput) throws IOException {
    if (str != null) {
      Text.writeString(dataOutput, str);
    } else {
      Text.writeString(dataOutput, "");
    }
  }

  private void readDocFields(DataInput in) throws IOException {
    int len = in.readInt();
    if (len > 0) {
      for (int i = 0; i < len; i++) {
        String key = readString(in);// issue if this is empty?

        if (key != null) {
          String value = readString(in);
          if (value != null) {
            fields.put(key, value);
          }
        }
      }
    }
  }

  @Override
  public void setContent(byte[] data) {
    fields.put(CONTENT, data.toString());
  }

  @Override
  public LWDocument addField(String name, Object value) {
    // TODO: Field mapping??
    if ("body".equals(name)) {
      name = name + "_txt";
    }
    if (name.equals(ID)) {
      setId(value.toString());
    } else {
      fields.put(name, value.toString());
    }
    return this;
  }

  @Override
  public LWDocument addMetadata(String name, String value) {
    // TODO
    fields.put(name, value);
    return this;
  }

  @Override
  public LWDocument checkId(String idField, String missingId) {
    String id = fields.get(idField);
    if (id == null) {
      setId(missingId);
    } else {
      setId(id);
    }
    return this;
  }

  @Override
  public Object getFirstFieldValue(String name) {
    return fields.get(name);
  }

  @Override
  public String toString() {
    return "[" + id + "] = " + fields.toString();
  }

  @Override
  public SolrInputDocument convertToSolr() {
    SolrInputDocument solrDoc = new SolrInputDocument();

    solrDoc.setField("id", getId());

    // Fields
    for (Map.Entry<String, String> field : fields.entrySet()) {
      solrDoc.addField(field.getKey().replace('.', '_'), field.getValue());
    }

    return solrDoc;
  }

  @Override
  public boolean equals(Object other) {
    LWMockDocument otherDoc = (LWMockDocument) other;
    boolean r = id.equals(otherDoc.getId());
    if (!r) {
      return false;
    }
    for (Map.Entry<String, String> field : otherDoc.getFields().entrySet()) {
      if (!fields.containsKey(field.getKey())) {
        return false;
      }
    }
    return true;
  }

}
