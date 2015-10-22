package com.lucidworks.hadoop.io.impl;

import com.lucidworks.hadoop.io.LWDocument;
import com.lucidworks.hadoop.io.impl.tika.TikaParsing;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.TimeZone;
import org.apache.hadoop.io.Text;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LWSolrDocument implements LWDocument {

  private static final String RAW_CONTENT = "_raw_content_bin";
  private static final String ID = "id";
  private static final String DUMMY_HOLDER = "--NA--";
  private transient static Logger log = LoggerFactory.getLogger(LWSolrDocument.class);

  private SolrInputDocument document;
  private static final ObjectMapper objectMapper = createMapper();

  public LWSolrDocument() {
    document = new SolrInputDocument();
  }

  public LWSolrDocument(String id, Map<String, String> metadata) {
    document = new SolrInputDocument();
    setId(id);
    if (metadata != null) {
      for (Map.Entry<String, String> entry : metadata.entrySet()) {
        document.addField(entry.getKey(), entry.getValue());
      }
    }
  }

  @Override
  public LWDocument[] process() {

    return new LWDocument[] { this };
  }

  @Override
  public void setId(String id) {
    document.setField(ID, id);
  }

  @Override
  public String getId() {
    SolrInputField id = document.get(ID);
    return id.getFirstValue().toString();
  }

  @Override
  public Map<String, String> getMetadata() {
    // TODO:
    return Collections.emptyMap();
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    writeString(getId(), dataOutput);
    writeDocFields(document, dataOutput);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    String id = readString(in);
    document = new SolrInputDocument();
    setId(id);
    readDocFields(document, in);
  }

  private void writeDocFields(SolrInputDocument doc, DataOutput dataOutput) throws IOException {
    if (doc != null && !doc.isEmpty()) {
      dataOutput.writeInt(doc.size());
      for (Map.Entry<String, SolrInputField> entry : doc.entrySet()) {
        if (!ID.equals(entry.getKey())) {
          writeString(entry.getKey(), dataOutput);
          SolrInputField solrField = entry.getValue();
          writeString(solrField.getName(), dataOutput);
          dataOutput.writeInt(solrField.getValueCount());
          for (Iterator<Object> iterator = solrField.iterator(); iterator.hasNext(); ) {
            Object theVal = iterator.next();
            if (theVal != null) {
              String valAsString = objectMapper.writeValueAsString(theVal);
              writeString(valAsString, dataOutput);
            } else {
              writeString(DUMMY_HOLDER, dataOutput);
              // we have no object, is this an error?
            }
          }
        }
      }
    } else {
      dataOutput.writeInt(0);
    }
  }

  private void readDocFields(SolrInputDocument doc, DataInput in) throws IOException {
    int len = in.readInt();
    if (len > 0) {
      for (int i = 0; i < len; i++) {
        String key = readString(in);// issue if this is empty?
        if (key != null) {
          String solrFieldName = readString(in);
          if (solrFieldName == null) {
            solrFieldName = key;
          }
          SolrInputField solrField = new SolrInputField(solrFieldName);
          int solrFieldlen = in.readInt();
          for (int j = 0; j < solrFieldlen; j++) {
            String theValStr = readString(in);// stored as JSON
            if (theValStr != null && !theValStr.equals(DUMMY_HOLDER)) {
              Object theVal = objectMapper.readValue(theValStr, Object.class);
              if (theVal != null) {
                solrField.addValue(theVal, 0);
              } else {
                log.warn("Couldn't convert JSON string: {}", theValStr);
              }
            }
          }
        }
      }
    }
  }

  private void writeString(String str, DataOutput dataOutput) throws IOException {
    if (str != null) {
      Text.writeString(dataOutput, str);
    } else {
      Text.writeString(dataOutput, "");
    }
  }

  private String readString(DataInput in) throws IOException {
    return Text.readString(in);
  }

  @Override
  public void setContent(byte[] data) {
    // TODO check error: ie data to big?
    TikaParsing.parseLWSolrDocument(this, data);
    document.addField(RAW_CONTENT, data);
  }

  @Override
  public LWDocument addField(String name, Object value) {
    // TODO field mapping?
    document.addField(name, value);
    return this;
  }

  @Override
  public LWDocument addMetadata(String name, String value) {
    document.addField(name, value);
    return this;
  }

  @Override
  public LWDocument checkId(String idField, String missingId) {
    SolrInputField id = document.get(idField);
    if (id == null) {
      setId(missingId);
    } else {
      setId(id.getName());
    }
    return this;
  }

  @Override
  public Object getFirstFieldValue(String name) {
    SolrInputField field = document.get(name);
    return field.getFirstValue();
  }

  @Override
  public SolrInputDocument convertToSolr() {
    return document;
  }

  @Override
  public boolean equals(Object other) {
    return convertToSolr().equals(((LWSolrDocument) other).convertToSolr());
  }

  private static ObjectMapper createMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
    mapper.configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS, false);
    mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    final DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    df.setTimeZone(TimeZone.getTimeZone("UTC"));
    mapper.setDateFormat(df);
    return mapper;
  }

}
