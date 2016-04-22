# kafka-connect-solr

Kafka Connect Solr for writing data to Solr. The SolrSinkTask can be configured with the following configuration.

<pre>
<code>
solr.cluster.url=http://localhost:8983/solr/kafka-solr/update/json?commit=true
solr.cluster.name=kafka-solr-sink-sample
bulk.size=500
action.type=insert
solr.doc.converter=com.apache.kafka.connect.solr.converter.SolrJsonDocConverter
</code>
</pre>

`solr.doc.converter` is a `com.apache.kafka.connect.solr.converter.Converter` that needs to be configured. This will take a SinkRecord object and serialize it in to JSON bytes that can be written to Solr.

If the data in Kafka is already in JSON format then you can use the `com.apache.kafka.connect.solr.converter.SolrJsonDocConverter` that is available with this library.

If the data in Kafka is in Avro format then you can use the `com.apache.kafka.connect.solr.converter.SolrAvroJsonDocConverter` that is available with this library.

