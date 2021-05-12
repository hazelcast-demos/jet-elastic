package org.hazelcast.cdc;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.mysql.MySqlCdcSources;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;

public class StreamPersonToStdOut {

    private static final String DB_SERVER_NAME = "legacy";
    private static final String DB_SCHEMA = "person";
    private static final String DB_NAMESPACED_TABLE = DB_SCHEMA + ".person";

    public static void main(String[] args) {
        var stream = new StreamPersonToStdOut();
        var pipeline = stream.pipeline();
        var client = HazelcastClient.newHazelcastClient();
        client.getJet().newJob(pipeline, stream.config());
        client.shutdown();
    }

    private JobConfig config() {
        var config = new JobConfig();
        config.addPackage(StreamPersonToStdOut.class.getPackageName());
        return config;
    }

    private Pipeline pipeline() {
        var pipeline = Pipeline.create();
        pipeline.readFrom(mysql())
                .withIngestionTimestamps()
                .writeTo(elasticsearch());
        return pipeline;
    }

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

    private Sink<Object> elasticsearch() {
        return Sinks.logger();
    }
}
