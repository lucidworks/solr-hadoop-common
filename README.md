# solr-hadoop-common
Shared code for our Hadoop ecosystem connectors

To build Solr hadoop common dependencies run Gradle, it Will publish into maven local.

```
./gradlew clean dist --info
```

Assuming the build succeeds the resulting artifacts will be in each subproject "build/libs" folder.

Runnig Tests

```
./gradlew test jacocoTestReport --info
```

The test report in html will be store in each subpoject `build/jacocoHtml` if the subproject has tests.

solr-hadoop-io
--------------
Map Reduce Input and Output format.


solr-hadoop-document
--------------------

Implemens the LWDocument inteface.


solr-hadoop-testbase
--------------------
The `SolrCloudClusterSupport` creates a `MiniSolrCloudCluster` in order to tests the application.