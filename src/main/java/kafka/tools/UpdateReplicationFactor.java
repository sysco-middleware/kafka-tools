package kafka.tools;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.*;
import java.util.stream.Collectors;

public class UpdateReplicationFactor {

    public static void main(String[] args) throws Exception {
        Config config = ConfigFactory.load();

        Properties adminConfig = new Properties();
        adminConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrap-servers"));

        AdminClient adminClient = AdminClient.create(adminConfig);

        if (args.length != 2)
            throw new IllegalArgumentException("Wrong args. execute command with args: <topic name> <target replication factor>");

        String topicName = args[0];
        Short targetReplicationFactor = Short.valueOf(args[1]);

        TopicDescription topicDescription =
                adminClient.describeTopics(Collections.singletonList(topicName)).all().get().get(topicName);
        Collection<Node> nodes = adminClient.describeCluster().nodes().get();

        if (targetReplicationFactor > nodes.size())
            throw new IllegalArgumentException("target replication-factor > cluster size");

        PartitionReassignmentJson.Builder builder = PartitionReassignmentJson.newBuilder();

        List<Integer> allReplicas = nodes.stream().map(Node::id).collect(Collectors.toList());

        for (TopicPartitionInfo tp : topicDescription.partitions()) {
            List<Integer> currentReplicas =
                    tp.replicas().stream().map(Node::id).collect(Collectors.toList());
            for (int i = currentReplicas.size(); i < targetReplicationFactor; i++) {
                List<Integer> list = allReplicas.stream()
                        .filter(id -> !currentReplicas.contains(id)).collect(Collectors.toList());
                Random r = new Random();
                currentReplicas.add(list.get(r.nextInt(list.size())));
            }
            builder.addPartition(
                    new PartitionReassignmentJson.Partition(topicDescription.name(), tp.partition(), currentReplicas));
        }
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        System.out.println(gson.toJson(builder.build()));
    }
}
