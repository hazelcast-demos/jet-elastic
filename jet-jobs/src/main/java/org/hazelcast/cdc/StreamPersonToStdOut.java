package org.hazelcast.cdc;

import com.hazelcast.core.Hazelcast;
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
        var instance = Hazelcast.bootstrappedInstance();
        instance.getJet().newJob(pipeline, stream.config());
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
        return MySqlCdcSources.mysql("mysql")
                .setDatabaseAddress(env.getOrDefault("MYSQL_HOST", "localhost"))
                .setDatabasePort(Integer.parseInt(env.getOrDefault("MYSQL_PORT", "3306")))
                .setDatabaseUser(env.getOrDefault("MYSQL_USER", "root"))
                .setDatabasePassword(env.getOrDefault("MYSQL_PASSWORD", "root"))
                .setClusterName(DB_SERVER_NAME)
                .setDatabaseWhitelist(DB_SCHEMA)
                .setTableWhitelist(DB_NAMESPACED_TABLE)
                .build();
    }

    private Sink<Object> elasticsearch() {
        return Sinks.logger();
    }
}
