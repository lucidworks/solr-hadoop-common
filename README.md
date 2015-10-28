# solr-hadoop-common

Shared code for our Hadoop ecosystem connectors

To build Solr hadoop common dependencies run Gradle.

```
./gradlew clean dist --info
```

Assuming the build succeeds the resulting artifacts will be publish into maven local.

Running Tests

```
./gradlew test jacocoTestReport --info
```

The test report in html format wil be store in each project build folder.

solr-hadoop-io
--------------
Map Reduce Input (`LWMapRedInputFormat`) and Output (`LWMapReduceOutputFormat`) format.

solr-hadoop-document
--------------------

Implement the `LWDocument` interface base on `SolrInputDocument`.

TODO: Tika processing,field mapping.

solr-hadoop-testbase
--------------------

Provides with the class `SolrCloudClusterSupport` base of most tests that needs an Embedded Solr Cluster for testing.
The `SolrCloudClusterSupport` creates a `MiniSolrCloudCluster` in order to tests the application.

`LWMockDocument` a mock Document to test the Solr indexing workflow.