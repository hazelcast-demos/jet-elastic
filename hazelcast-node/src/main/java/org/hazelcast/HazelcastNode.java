package org.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;

public class HazelcastNode {

    public static void main(String[] args) {
        var config = new Config();
        var jetConfig = config.getJetConfig();
        jetConfig.setEnabled(true);
        jetConfig.setResourceUploadEnabled(true);
        Hazelcast.newHazelcastInstance(config);
    }
}
