package org.hazelcast.cdc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import com.hazelcast.function.BiFunctionEx;
import com.zaxxer.hikari.HikariDataSource;
import org.json.JSONObject;

public class WithMarketingLabels implements BiFunctionEx<HikariDataSource, JSONObject, JSONObject> {

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
    public JSONObject applyEx(HikariDataSource pool, JSONObject json) throws SQLException {
        try (var connection = pool.getConnection();
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
