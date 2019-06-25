package kafka.tools;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import kafka.tools.internal.KafkaAdminClientSupplier;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

public class UpdateReplicationFactor {

    AdminClient adminClient = KafkaAdminClientSupplier.create();

    public static void main(String[] args) throws Exception {
        List<String> lines = Files.readAllLines(Paths.get("topics-to-reassign"));

        UpdateReplicationFactor tool = new UpdateReplicationFactor();

        final String targetDir = "target/reassignment";

        if (Files.notExists(Paths.get(targetDir))) {
            new File(targetDir).mkdir();
        } else {
            new File(targetDir).delete();
        }

        for (String topicName : lines) {
            String json = tool.replicationFactorJson(topicName, (short) 3);
            Files.write(Paths.get(targetDir + "/" + topicName + ".json"), json.getBytes(UTF_8));
        }
    }

    public String replicationFactorJson(String topicName, Short targetReplicationFactor) throws ExecutionException, InterruptedException {
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
        final String json = gson.toJson(builder.build());
        return json;
    }
}
