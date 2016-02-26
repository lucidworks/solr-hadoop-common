package com.lucidworks.hadoop.io;

import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import com.lucidworks.hadoop.clients.FusionPipelineClientTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.Assert.*;

import org.codehaus.jackson.map.ObjectMapper;

import java.util.List;
import java.util.Map;

public class FusionOutputFormatTest extends FusionPipelineClientTest {

  @Override
  @Test
  public void test() throws Exception {
    // mock out the Pipeline API
    stubFor(post(urlEqualTo(fusionPipelineUrlWithoutHostAndPort)).willReturn(aResponse().withStatus(200)));

    // mock out the Session API
    stubFor(post(urlEqualTo("/api/session?realmName=" + fusionRealm)).willReturn(aResponse().withStatus(200)));

    Configuration conf = new Configuration();
    conf.set("fusion.endpoints", fusionUrl);
    conf.set("fusion.user", fusionUser);
    conf.set("fusion.pass", fusionPass);
    conf.set("fusion.realm", fusionRealm);

    FusionOutputFormat outputFormat = new FusionOutputFormat();
    RecordWriter<Text, LWDocumentWritable> writer =
            outputFormat.getRecordWriter(null, new JobConf(conf), "test", Reporter.NULL);
    writer.write(new Text("text"), LucidWorksWriterTest
            .createLWDocumentWritable("id-1", "field-1", "field-value-1", "field-2", "field-value-2", "field-3", "field-value-3"));
    writer.close(Reporter.NULL);

    String docsSentToFusion = null;
    for (LoggedRequest req : loggedRequests) {
      String body = req.getBodyAsString().trim();
      if (body.startsWith("[") && body.endsWith("]")) {
        docsSentToFusion = body;
        break;
      }
    }

    assertNotNull("Expected 1 doc to have been written by FusionOutputFormat", docsSentToFusion);

    ObjectMapper objectMapper = new ObjectMapper();
    List docs = objectMapper.readValue(docsSentToFusion, List.class);
    assertTrue(docs.size() == 1);
    Map<String,Object> doc = (Map<String,Object>)docs.get(0);
    assertNotNull(doc);
    assertEquals(doc.get("id"), "id-1");
    List<Map<String,Object>> fields = (List<Map<String,Object>>)doc.get("fields");
    assertTrue(!fields.isEmpty());

    String[] expectedFields = new String[]{ "field-1", "field-2", "field-3" };
    for (String f : expectedFields) {
      boolean foundIt = false;
      for (Map<String,Object> map : fields) {
        if (f.equals(map.get("name"))) {
          foundIt = true;
          break;
        }
      }
      if (!foundIt)
        fail("Expected field "+f+" not found in doc sent to Fusion!");
    }
  }
}
