# solr-hadoop-common

Shared code for our Hadoop ecosystem connectors

The project is designed to a submodule, in order to build it go one of the main projects and build it from there.

Main projects
* https://github.com/LucidWorks/hadoop-solr
* https://github.com/LucidWorks/hive-solr
* https://github.com/LucidWorks/pig-solr

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