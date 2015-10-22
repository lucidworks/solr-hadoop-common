package com.lucidworks.hadoop.io;

import com.lucidworks.hadoop.utils.SolrCloudClusterSupport;
import com.lucidworks.hadoop.utils.TestUtils;
import java.io.IOException;
import java.net.SocketException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Progressable;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.util.NamedList;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LucidWorksWriterTest extends SolrCloudClusterSupport {

  private transient static Logger log = LoggerFactory.getLogger(LucidWorksWriterTest.class);

  // TODO
  //private final String COLLECTION = "com.lucidworks.hadoop.ingest.collection";

  @Before
  public void setUp() throws Exception {
    removeAllDocs();
  }

  @Test
  public void test() throws IOException, SolrServerException {
    MockProgressable progressable = new MockProgressable();
    LucidWorksWriter lucidWorksWriter = new LucidWorksWriter(progressable);

    Configuration conf = new Configuration();
    conf.set(LucidWorksWriter.SOLR_ZKHOST, getBaseUrl());
    conf.set("lww.commit.on.close", "true");
    conf.setBoolean("lw.annotations", true);
    conf.setBoolean("lw.metadata", true);
    conf.set("solr.f.junk_annot_1_s",
        "foo.annot_1");//map the annot_1 feature of foo annotation to the junk_annot_1_s field
    //conf.set(COLLECTION, "collection1");

    lucidWorksWriter.open(conf, "name");

    lucidWorksWriter.write(new Text("text"), TestUtils
        .createPipelineDocumentWritable("id-1", "field-1", "field-value-1", "field-2",
            "field-value-2", "field-3", "field-value-3"));

    LWDocumentWritable doc = TestUtils
        .createPipelineDocumentWritable("id-2", "field-1", "This is field value 1", "field-2_ss",
            "This is field value 2.  It is longer than field value 1.", "field-3",
            "This is field value 3.  It is longer than both field value 1 and field value 2.");
    //add annotations
    LWDocument pipeDoc = doc.getLWDocument();
    pipeDoc.addField("field-2_ss", "This is a 2nd entry for field value 2");
    //    PipelineField pField = pipeDoc.getFirstField("field-3");
    //    for (int i = 0; i < 5; i++) {
    //      Map<String, String> features = new HashMap<String, String>();
    //      features.put("annot_" + i, "annotation_" + i);
    //      pField.addAnnotation(new Annotation("foo", i, i + 1, features));
    //    }
    //add metadata
    for (int i = 0; i < 6; i++) {
      pipeDoc.addMetadata("meta_" + i, "meta_value_" + i);
    }
    lucidWorksWriter.write(new Text("text"), doc);
    lucidWorksWriter.commit();
    lucidWorksWriter.close();

    assertQ("id:id-1", 1, "field-1", "field-value-1", "field-2", "field-value-2", "field-3",
        "field-value-3");
    assertQ("*:*", 2);
    //Metadata
    assertQ("meta_0:meta_value_0", 1);
    //Annotations
    // TODO: MOCK without annotations.
    //assertQ("junk_annot_1_s:annotation_1", 1);
  }

  @Test
  public void testRetry() throws Exception {
    MockProgressable progressable = new MockProgressable();
    LucidWorksWriter lucidWorksWriter = new LucidWorksWriter(progressable);
    Configuration conf = new Configuration();
    conf.set(LucidWorksWriter.SOLR_ZKHOST, getBaseUrl());
    conf.set("lww.commit.on.close", "true");
    //conf.set(COLLECTION, "collection1");
    conf.setInt("lww.retry.sleep.time", 1);//make the sleep really short
    lucidWorksWriter.open(conf, "name");
    //make sure we trigger the buffering
    int totalDocs = 5000;
    for (int counter = 0; counter < totalDocs; counter++) {
      lucidWorksWriter.write(new Text("text-" + counter), TestUtils
          .createPipelineDocumentWritable("id-" + counter, "field-1", "field-value-1", "field-2",
              "field-value-2", "field-3", "field-value-3"));

    }
    lucidWorksWriter.close();
    assertCount("*:*", totalDocs);
  }

  @Test(expected = IOException.class)
  public void testFail() throws Exception {
    MockProgressable progressable = new MockProgressable();
    SolrClient fakeServer = new FakeRetrySolrServer(new HttpSolrClient(getBaseUrl()), true);
    LucidWorksWriter lucidWorksWriter = new LucidWorksWriter(fakeServer, progressable);
    Configuration conf = new Configuration();
    conf.set(LucidWorksWriter.SOLR_ZKHOST, getBaseUrl());
    conf.set("lww.commit.on.close", "true");
    //conf.set(COLLECTION, "collection1");
    conf.set("solr.params", "key1=val1&key2=val2");
    conf.setInt("lww.retry.sleep.time", 1);//make the sleep really short
    lucidWorksWriter.open(conf, "name");
    //try {
    lucidWorksWriter.write(new Text("text-1"), TestUtils
        .createPipelineDocumentWritable("id-1", "field-1", "field-value-1", "field-2",
            "field-value-2", "field-3", "field-value-3"));
    lucidWorksWriter.close();//close here
  }

  @Test
  public void testManyDocs() throws IOException, SolrServerException {
    MockProgressable progressable = new MockProgressable();
    LucidWorksWriter lucidWorksWriter = new LucidWorksWriter(progressable);

    Configuration conf = new Configuration();
    conf.set(LucidWorksWriter.SOLR_ZKHOST, getBaseUrl());
    conf.set("lww.commit.on.close", "true");
    //conf.set(COLLECTION, "collection1");
    lucidWorksWriter.open(conf, "name");
    //make sure we trigger the buffering and have left overs
    int totalDocs = 5051;
    for (int counter = 0; counter < totalDocs; counter++) {
      lucidWorksWriter.write(new Text("text-" + counter), TestUtils
          .createPipelineDocumentWritable("id-" + counter, "field-1", "field-value-1", "field-2",
              "field-value-2", "field-3", "field-value-3"));

    }
    lucidWorksWriter.close();
    assertCount("*:*", totalDocs);

  }

  public class MockProgressable implements Progressable {

    @Override
    public void progress() {
      // Do nothing, just for test
    }
  }

  @SuppressWarnings("deprecation")
  private class FakeRetrySolrServer extends SolrClient {
    private final SolrClient delegate;
    int calls = 0;
    private boolean alwaysFail;

    public FakeRetrySolrServer(SolrClient server, boolean alwaysFail) {
      this.delegate = server;
      this.alwaysFail = alwaysFail;
    }

    @Override
    public NamedList<Object> request(SolrRequest request, String collection)
        throws SolrServerException, IOException {
      if (alwaysFail) {
        calls++;
        throw new SolrServerException("alwaysFail is true");
      }
      if (calls < 2) {
        Throwable cause = new SocketException("fake socket exception to cause a retry");
        calls++;
        throw new SolrServerException("calls = " + calls, cause);
      }
      calls++;
      return delegate.request(request);
    }

    @Override
    public void shutdown() {
      try {
        delegate.close();
      } catch (IOException e) {
        log.warn("closing");
      }
    }
  }
}
