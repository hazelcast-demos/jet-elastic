package org.hazelcast.cdc;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class HikariConnectionPool implements FunctionEx<ProcessorSupplier.Context, HikariDataSource> {

    private final String user;
    private final String password;
    private final String url;

    public HikariConnectionPool() {
        var env = System.getenv();
        this.user = env.getOrDefault("MYSQL_USER", "root");
        this.password = env.getOrDefault("MYSQL_PASSWORD", "root");
        this.url = "jdbc:mysql://" + env.getOrDefault("MYSQL_HOST", "localhost") + ":" + env.getOrDefault("MYSQL_PORT", "3306") + "/person";
    }

    @Override
    public HikariDataSource applyEx(ProcessorSupplier.Context ctx) {
        var config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(user);
        config.setPassword(password);
        return new HikariDataSource(config);
    }
}
