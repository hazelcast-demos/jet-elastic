package org.hazelcast.cdc;

import com.hazelcast.client.HazelcastClient;

public class StopStream {

    public static void main(String[] args) {
        var client = HazelcastClient.newHazelcastClient();
        var job = client.getJet().getJob("stream-persons-to-elasticsearch");
        if (job != null) {
            job.cancel();
        }
        client.shutdown();
    }
}
