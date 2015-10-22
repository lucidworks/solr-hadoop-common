package com.lucidworks.hadoop.io;

import com.lucidworks.hadoop.utils.SolrCloudClusterSupport;
import java.io.IOException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
import org.junit.Test;

public class LWMapRedInputFormatTest extends SolrCloudClusterSupport {

  @Test
  public void testInputFormat() throws IOException {
    JobConf conf = new JobConf();
    //    conf.set(LWMapRedInputFormat.SOLR_ZKHOST, getBaseUrl());
    //    conf.set(LWMapRedInputFormat.SOLR_COLLECTION, "collection1");
    //    conf.set("lww.commit.on.close", "true");
    //
    //    // lots of docs
    //    int docsCount = 10005;
    //
    //    LucidWorksWriter writer = new LucidWorksWriter(new MockProgressable());
    //    writer.open(conf, "test");
    //    for (int i=0;i<docsCount;i++) {
    //      writer.write(new Text("text"+i), TestUtils
    //          .createPipelineDocumentWritable("id" + i, "field1", "fieldValue1", "field2", "fieldValue2"));
    //    }
    //    writer.close();
    //
    //    LWMapRedInputFormat inputFormat = new LWMapRedInputFormat();
    //    InputSplit[] splits = inputFormat.getSplits(conf, 0);
    //    RecordReader<IntWritable, PipelineDocumentWritable> reader = inputFormat
    //            .getRecordReader(splits[0], conf, null);
    //
    //    IntWritable key = reader.createKey();
    //    PipelineDocumentWritable value = reader.createValue();
    //    int count = 0;
    //    while (reader.next(key, value)) {
    //      count++;
    //    }
    //    reader.close();
    //
    //    assertEquals(docsCount, count);
  }

  public static class MockProgressable implements Progressable {
    @Override
    public void progress() {
    }
  }
}
