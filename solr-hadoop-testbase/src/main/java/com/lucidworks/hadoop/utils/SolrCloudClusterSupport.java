package com.lucidworks.hadoop.utils;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakAction;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakZombies;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.noggit.CharArr;
import org.noggit.JSONWriter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@ThreadLeakAction({ ThreadLeakAction.Action.WARN })
@ThreadLeakLingering(linger = 0)
@ThreadLeakZombies(ThreadLeakZombies.Consequence.CONTINUE)
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class SolrCloudClusterSupport {

  static final Logger log = Logger.getLogger(SolrCloudClusterSupport.class);

  protected static MiniSolrCloudCluster cluster;
  protected static CloudSolrClient cloudSolrServer;
  public static File TEMP_DIR;
  public static final String DEFAULT_COLLECTION = "collection1";

  @BeforeClass
  public static void startCluster() throws Exception {

    File solrXml = new File(ClassLoader.getSystemClassLoader().getResource("solr.xml").getPath());

    TEMP_DIR = Files.createTempDirectory("MiniSolrCloudCluster").toFile();
    cluster = new MiniSolrCloudCluster(1, null, TEMP_DIR, solrXml, null, null);
    cloudSolrServer = new CloudSolrClient(cluster.getZkServer().getZkAddress(), true);
    cloudSolrServer.setDefaultCollection("collection1");

    cloudSolrServer.connect();
    assertTrue(!cloudSolrServer.getZkStateReader().getClusterState().getLiveNodes().isEmpty());

    createDefaultCollection();
    verifyCluster();
  }

  @AfterClass
  public static void stopCluster() throws Exception {
    cloudSolrServer.close();
    cluster.shutdown();
    if (TEMP_DIR != null) {
      TEMP_DIR.delete();
    }
    TEMP_DIR = null;
    cluster = null;
    cloudSolrServer = null;
  }

  private static void createDefaultCollection() throws Exception {
    String confName = "testConfig";
    File confDir = new File(ClassLoader.getSystemClassLoader().getResource("conf").getPath());
    int numShards = 1;
    int replicationFactor = 1;

    createCollection(DEFAULT_COLLECTION, numShards, replicationFactor, confName, confDir);
  }

  private static void verifyCluster() throws IOException, SolrServerException {
    // Ping cluster to verify
    SolrPingResponse response = cloudSolrServer.ping();
    assertEquals(response.getStatus(), 0);
  }

  protected static void createCollection(String collectionName, int numShards,
      int replicationFactor, String confName) throws Exception {
    createCollection(collectionName, numShards, replicationFactor, confName, null);
  }

  protected static void createCollection(String collectionName, int numShards,
      int replicationFactor, String confName, File confDir) throws Exception {
    if (confDir != null) {
      assertTrue("Specified Solr config directory '" +
          confDir.getAbsolutePath() + "' not found!", confDir.isDirectory());

      // upload the test configs
      SolrZkClient zkClient = cloudSolrServer.getZkStateReader().getZkClient();
      ZkConfigManager configManager = new ZkConfigManager(zkClient);
      configManager.uploadConfigDir(confDir.toPath(), confName);
    }

    ModifiableSolrParams modParams = new ModifiableSolrParams();
    modParams.set(CoreAdminParams.ACTION, CollectionParams.CollectionAction.CREATE.name());
    modParams.set("name", collectionName);
    modParams.set("numShards", numShards);
    modParams.set("replicationFactor", replicationFactor);
    modParams.set("collection.configName", confName);
    QueryRequest request = new QueryRequest(modParams);
    request.setPath("/admin/collections");
    cloudSolrServer.request(request);
    ensureAllReplicasAreActive(collectionName, numShards, replicationFactor, 20);
  }

  protected static void ensureAllReplicasAreActive(String testCollectionName, int shards, int rf,
      int maxWaitSecs) throws Exception {
    long startMs = System.currentTimeMillis();

    ZkStateReader zkr = cloudSolrServer.getZkStateReader();
    zkr.updateClusterState(true); // force the state to be fresh

    ClusterState cs = zkr.getClusterState();
    Collection<Slice> slices = cs.getActiveSlices(testCollectionName);
    assertTrue(slices.size() == shards);
    boolean allReplicasUp = false;
    long waitMs = 0L;
    long maxWaitMs = maxWaitSecs * 1000L;
    Replica leader = null;
    while (waitMs < maxWaitMs && !allReplicasUp) {
      // refresh state every 2 secs
      if (waitMs % 2000 == 0) {
        log.info("Updating ClusterState");
        cloudSolrServer.getZkStateReader().updateClusterState(true);
      }

      cs = cloudSolrServer.getZkStateReader().getClusterState();
      assertNotNull(cs);
      allReplicasUp = true; // assume true
      for (Slice shard : cs.getActiveSlices(testCollectionName)) {
        String shardId = shard.getName();
        assertNotNull("No Slice for " + shardId, shard);
        Collection<Replica> replicas = shard.getReplicas();
        assertTrue(replicas.size() == rf);
        leader = shard.getLeader();
        assertNotNull(leader);
        log.info("Found " + replicas.size() + " replicas and leader on " +
            leader.getNodeName() + " for " + shardId + " in " + testCollectionName);

        // ensure all replicas are "active"
        for (Replica replica : replicas) {
          Replica.State replicaState = replica.getState();
          if (!Replica.State.ACTIVE.equals(replicaState)) {
            log.info("Replica " + replica.getName() + " for shard " + shardId + " is currently "
                + replicaState);
            allReplicasUp = false;
          }
        }
      }

      if (!allReplicasUp) {
        try {
          Thread.sleep(500L);
        } catch (Exception ignoreMe) {
        }
        waitMs += 500L;
      }
    } // end while

    if (!allReplicasUp) {
      fail("Didn't see all replicas for " + testCollectionName +
          " come up within " + maxWaitMs + " ms! ClusterState: " + printClusterStateInfo(
          testCollectionName));
    }
    long diffMs = (System.currentTimeMillis() - startMs);
    log.info("Took " + diffMs + " ms to see all replicas become active for " + testCollectionName);
  }

  protected static String printClusterStateInfo(String collection) throws Exception {
    cloudSolrServer.getZkStateReader().updateClusterState(true);
    String cs = null;
    ClusterState clusterState = cloudSolrServer.getZkStateReader().getClusterState();
    if (collection != null) {
      cs = clusterState.getCollection(collection).toString();
    } else {
      Map<String, DocCollection> map = new HashMap<String, DocCollection>();
      for (String coll : clusterState.getCollections())
        map.put(coll, clusterState.getCollection(coll));
      CharArr out = new CharArr();
      new JSONWriter(out, 2).write(map);
      cs = out.toString();
    }
    return cs;
  }

  protected static String getBaseUrl() {
    return cluster.getZkServer().getZkAddress();
  }

  protected static String getZkHost() {
    return cluster.getZkServer().getZkHost();
  }

  protected static void removeAllDocs() throws IOException, SolrServerException {
    cloudSolrServer.deleteByQuery("*:*");
  }

  // Use to verify document count
  protected void assertCount(String query, int expectedCount)
      throws SolrServerException, IOException {

    SolrQuery solrQuery = new SolrQuery();
    solrQuery.setQuery(query);

    QueryResponse rsp = cloudSolrServer.query(solrQuery);
    SolrDocumentList docs = rsp.getResults();
    int docsSize = (int) docs.getNumFound();

    assertEquals("expected " + expectedCount + "  obtained: " + docsSize, docsSize, expectedCount);

  }

  protected void assertQ(String query, int expectedCount, String... expectedFields)
      throws SolrServerException, IOException {

    SolrQuery solrQuery = new SolrQuery();
    solrQuery.setQuery(query);

    QueryResponse rsp = cloudSolrServer.query(solrQuery);
    SolrDocumentList docs = rsp.getResults();
    assertEquals(expectedCount, docs.size());
    log.info("docs: " + docs.toString());

    if (docs.isEmpty()) {
      fail("No results for query " + query);
    }

    SolrDocument doc = docs.get(0);
    if (expectedFields != null) {
      for (int counter = 0; counter < expectedFields.length; counter += 2) {
        assertTrue(
            doc.getFieldValue(expectedFields[counter]).toString() + " != " + expectedFields[counter
                + 1], doc.getFieldValue(expectedFields[counter]).toString()
                .equals(expectedFields[counter + 1]));
      }
    }
  }
}