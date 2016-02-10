package com.lucidworks.hadoop.io;

import com.lucidworks.hadoop.clients.FusionPipelineClient;
import com.lucidworks.hadoop.utils.Security;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class FusionOutputFormat implements OutputFormat<Text, LWDocumentWritable> {

  private transient static Logger log = LoggerFactory.getLogger(FusionOutputFormat.class);

  public static class FusionRecordWriter implements RecordWriter<Text, LWDocumentWritable> {

    private final String jobName;
    private final FusionPipelineClient fusionPipelineClient;
    private final Progressable progressable;
    private final DocBuffer docBuffer;
    private ObjectMapper jsonObjectMapper;

    public FusionRecordWriter(Configuration job, String name, Progressable progressable) throws IOException {
      this.jobName = name;
      this.progressable = progressable;

      Security.setSecurityConfig(job);

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

      jsonObjectMapper = new ObjectMapper();
    }

    @Override
    public void write(Text text, LWDocumentWritable documentWritable) throws IOException {
      docBuffer.add(documentWritable.getLWDocument());

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
    public final List<LWDocument> buffer;
    public final long bufferTimeoutMs;
    public final int maxBufferSize;

    private long bufferTimeoutAtNanos = -1L;

    public DocBuffer(int maxBufferSize, long bufferTimeoutMs) {
      this.maxBufferSize = maxBufferSize;
      this.bufferTimeoutMs = bufferTimeoutMs;
      this.buffer = new ArrayList<LWDocument>(maxBufferSize);
    }

    public void add(LWDocument doc) {
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