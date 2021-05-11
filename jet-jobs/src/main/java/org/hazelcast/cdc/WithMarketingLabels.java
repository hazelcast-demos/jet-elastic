package org.hazelcast.cdc;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import com.hazelcast.function.FunctionEx;
import org.json.JSONObject;

public class WithMarketingLabels implements FunctionEx<JSONObject, JSONObject> {

    private static final List<String> MARKETING = List.of(
            "cars",
            "electronic",
            "fashion",
            "food",
            "garden",
            "hifi",
            "music",
            "shoes",
            "toys"
    );

    @Override
    public JSONObject applyEx(JSONObject json) throws SQLException {
        var env = System.getenv();
        var host = env.getOrDefault("MYSQL_HOST", "localhost");
        var port = env.getOrDefault("MYSQL_PORT", "3306");
        var user = env.getOrDefault("MYSQL_USER", "root");
        var password = env.getOrDefault("MYSQL_PASSWORD", "root");
        try (var connection = DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "/person", user, password);
             var statement = connection.prepareStatement("SELECT * FROM marketing WHERE id = ?")) {
            var marketingId = json.getInt("marketing_id");
            statement.setInt(1, marketingId);
            var resultSet = statement.executeQuery();
            var richJson = json;
            if (resultSet.next()) {
                for (String marketing : MARKETING) {
                    richJson = enrichWith(richJson, resultSet, marketing);
                }
            }
            return richJson;
        }
    }

    private JSONObject enrichWith(JSONObject json, ResultSet resultSet, String marketing) throws SQLException {
        var value = resultSet.getInt(marketing);
        if (resultSet.wasNull()) {
            return json;
        } else if (value > 500) {
            var richJson = new JSONObject(json.toMap());
            if (!richJson.has("marketing")) {
                richJson.put("marketing", new ArrayList<>());
            }
            var array = richJson.getJSONArray("marketing");
            array.put(marketing);
            return richJson;
        }
        return json;
    }
}
