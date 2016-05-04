package com.lucidworks.hadoop.io;

import com.lucidworks.hadoop.clients.FusionPipelineClient;
import com.lucidworks.hadoop.security.SolrSecurity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class FusionOutputFormat implements OutputFormat<Text, LWDocumentWritable> {

  private transient static Logger log = LoggerFactory.getLogger(FusionOutputFormat.class);

  public static class FusionRecordWriter implements RecordWriter<Text, LWDocumentWritable> {

    private final FusionPipelineClient fusionPipelineClient;
    private final Progressable progressable;
    private final DocBuffer docBuffer;

    public FusionRecordWriter(Configuration job, String name, Progressable progressable) throws IOException {
      this.progressable = progressable;

      //SolrSecurity.setSecurityConfig(job);

      int batchSize = Integer.parseInt(job.get("fusion.batchSize", "500"));
      long bufferTimeoutMs = Long.parseLong(job.get("fusion.buffer.timeoutms", "1000"));
      this.docBuffer = new DocBuffer(batchSize, bufferTimeoutMs);

      boolean fusionAuthEnabled = "true".equals(job.get("fusion.authenabled", "true"));
      String fusionUser = job.get("fusion.user");
      String fusionPass = job.get("fusion.pass");
      String fusionRealm = job.get("fusion.realm");
      String endpoints = job.get("fusion.endpoints");

      Reporter reporter = null;
      if (progressable instanceof Reporter) {
        reporter = (Reporter) progressable;
      } else {
        reporter = Reporter.NULL;
        log.warn("Progressable passed to FusionOutputFormat is not a Reporter, so no counters will be provided.");
      }

      fusionPipelineClient = fusionAuthEnabled
          ? new FusionPipelineClient(reporter, endpoints, fusionUser, fusionPass, fusionRealm)
          : new FusionPipelineClient(reporter, endpoints);
    }

    @Override
    public void write(Text text, LWDocumentWritable documentWritable) throws IOException {
      docBuffer.add(doc2json(documentWritable.getLWDocument().convertToSolr()));

      if (docBuffer.shouldFlushBuffer()) {
        try {
          fusionPipelineClient.postBatchToPipeline(docBuffer.buffer);
          progressable.progress();
        } catch (Exception exc) {
          if (exc instanceof IOException) {
            throw (IOException) exc;
          } else {
            throw new IOException("Failed to send final batch to Fusion due to: " + exc, exc);
          }
        } finally {
          docBuffer.reset();
        }
      }
    }

    protected Map<String, Object> doc2json(SolrInputDocument solrDoc) {
      Map<String, Object> json = new HashMap<String, Object>();
      String docId = (String) solrDoc.getFieldValue("id");
      if (docId == null)
        throw new IllegalStateException("Couldn't resolve the id for document: " + solrDoc);
      json.put("id", docId);

      List fields = new ArrayList();
      for (String f : solrDoc.getFieldNames()) {
        if (!"id".equals(f)) { // id already added
          appendField(solrDoc, f, null, fields);
        }
      }
      // keep track of the time we saw this doc on the hbase side
      String tdt = DateUtil.getThreadLocalDateFormat().format(new Date());
      fields.add(mapField("_hadoop_tdt", null, tdt));

      json.put("fields", fields);
      return json;
    }

    protected void appendField(SolrInputDocument doc, String f, String pfx, List fields) {
      SolrInputField field = doc.getField(f);
      int vc = field.getValueCount();
      if (vc <= 0)
        return; // no values to add for this field

      if (vc == 1) {
        Map<String, Object> fieldMap = mapField(f, pfx, field.getFirstValue());
        if (fieldMap != null)
          fields.add(fieldMap);
      } else {
        for (Object val : field.getValues()) {
          Map<String, Object> fieldMap = mapField(f, pfx, val);
          if (fieldMap != null)
            fields.add(fieldMap);
        }
      }
    }

    protected Map<String, Object> mapField(String f, String pfx, Object val) {
      Map<String, Object> fieldMap = new HashMap<String, Object>(10);
      String fieldName = (pfx != null) ? pfx + f : f;
      fieldMap.put("name", fieldName);
      fieldMap.put("value", val);
      return fieldMap;
    }

    public void close(Reporter reporter) throws IOException {
      if (!docBuffer.buffer.isEmpty()) {
        try {
          fusionPipelineClient.postBatchToPipeline(docBuffer.buffer);
        } catch (Exception exc) {
          if (exc instanceof IOException) {
            throw (IOException) exc;
          } else {
            throw new IOException("Failed to send final batch of " + docBuffer.buffer.size() + " docs to Fusion due " +
                "to: " + exc, exc);
          }
        } finally {
          docBuffer.reset();
        }
      }
    }
  } // end FusionRecordWriter class

  public static class DocBuffer {
    public final List<Object> buffer;
    public final long bufferTimeoutMs;
    public final int maxBufferSize;

    private long bufferTimeoutAtNanos = -1L;

    public DocBuffer(int maxBufferSize, long bufferTimeoutMs) {
      this.maxBufferSize = maxBufferSize;
      this.bufferTimeoutMs = bufferTimeoutMs;
      this.buffer = new ArrayList<>(maxBufferSize);
    }

    public void add(Object doc) {
      buffer.add(doc);

      // start the timer when the first doc arrives in this batch
      if (bufferTimeoutAtNanos == -1L) {
        bufferTimeoutAtNanos = System.nanoTime() + TimeUnit.NANOSECONDS.convert(bufferTimeoutMs, TimeUnit.MILLISECONDS);
      }
    }

    public void reset() {
      bufferTimeoutAtNanos = -1L;
      buffer.clear();
    }

    public boolean shouldFlushBuffer() {
      if (buffer.isEmpty()) {
        return false;
      }
      return (buffer.size() >= maxBufferSize) || System.nanoTime() >= bufferTimeoutAtNanos;
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("DocBuffer: ").append(buffer.size()).append(", shouldFlush? ").append(shouldFlushBuffer());
      return sb.toString();
    }
  }

  public RecordWriter<Text, LWDocumentWritable> getRecordWriter(FileSystem fileSystem, JobConf jobConf, String name,
                                                                Progressable progressable) throws IOException {
    return new FusionRecordWriter(jobConf, name, progressable);
  }

  public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {
    // no-op
  }
}