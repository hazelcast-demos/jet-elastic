package org.hazelcast.cdc;

import java.util.Random;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.mysql.MySqlCdcSources;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.elastic.ElasticSinks;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.StreamSource;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClientBuilder;
import org.json.JSONObject;

import static com.hazelcast.jet.elastic.ElasticClients.client;

public class StreamPersonToElasticSearch {

    private static final String DB_SERVER_NAME = "legacy";
    private static final String DB_SCHEMA = "person";
    private static final String DB_NAMESPACED_TABLE = DB_SCHEMA + ".person";

    public static void main(String[] args) {
        var stream = new StreamPersonToElasticSearch();
        var pipeline = stream.pipeline();
        var client = HazelcastClient.newHazelcastClient();
        client.getJet().newJob(pipeline, stream.config());
        client.shutdown();
    }

    private JobConfig config() {
        var config = new JobConfig();
        config.addPackage(StreamPersonToElasticSearch.class.getPackageName());
        return config;
    }

    private Pipeline pipeline() {
        var pipeline = Pipeline.create();
        pipeline.readFrom(mysql())
                .withIngestionTimestamps()
                .map(toJson.andThen(new WithMarketingLabels()))
                .peek(json -> new Random().nextInt(10) == 0, peekJson)
                .writeTo(elasticsearch());
        return pipeline;
    }

    private final FunctionEx<ChangeRecord, JSONObject> toJson = change -> new JSONObject(change.value().toJson());

    private final FunctionEx<JSONObject, String> peekJson = json -> json.toString(4);

    private StreamSource<ChangeRecord> mysql() {
        var env = System.getenv();
        var host = env.getOrDefault("MYSQL_HOST", "localhost");
        var port = Integer.parseInt(env.getOrDefault("MYSQL_PORT", "3306"));
        var user = env.getOrDefault("MYSQL_USER", "root");
        var password = env.getOrDefault("MYSQL_PASSWORD", "root");
        return MySqlCdcSources.mysql("mysql")
                .setDatabaseAddress(host)
                .setDatabasePort(port)
                .setDatabaseUser(user)
                .setDatabasePassword(password)
                .setClusterName(DB_SERVER_NAME)
                .setDatabaseWhitelist(DB_SCHEMA)
                .setTableWhitelist(DB_NAMESPACED_TABLE)
                .build();
    }

    private Sink<JSONObject> elasticsearch() {
        var env = System.getenv();
        var user = env.getOrDefault("ELASTICSEARCH_USER", "user");
        var password = env.getOrDefault("ELASTICSEARCH_PASSWORD", "password");
        var host = env.getOrDefault("ELASTICSEARCH_HOST", "localhost");
        var port = Integer.parseInt(env.getOrDefault("ELASTICSEARCH_PORT", "9200"));
        SupplierEx<RestClientBuilder> clientFn = () -> client(user, password, host, port);
        FunctionEx<JSONObject, DocWriteRequest<?>> requestFn = json -> new IndexRequest("persons")
                .id(String.valueOf(json.get("id")))
                .source(json.toMap());
        return ElasticSinks.elastic(clientFn, requestFn);
    }
}
