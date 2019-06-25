package kafka.tools;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import kafka.tools.internal.KafkaAdminClientSupplier;
import kafka.tools.internal.Neo4jDriverSupplier;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class KafkaTopologyNeo4jConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTopologyNeo4jConnector.class);

    public static void main(String[] args) throws Exception {
        Config config = ConfigFactory.load();

        AdminClient adminClient = new KafkaAdminClientSupplier().get();

        DescribeClusterResult describeClusterResult =
                adminClient.describeCluster(new DescribeClusterOptions());

        Driver driver = new Neo4jDriverSupplier().get();

        String clusterName = config.getString("kafka.cluster-name");

        try (Session session = driver.session()) {
            final String clusterId = describeClusterResult.clusterId().get();

            indexCluster(describeClusterResult, clusterName, session, clusterId);

            indexTopics(adminClient, session, clusterId);
        }

        adminClient.close();
        driver.close();
    }

    private static void indexCluster(DescribeClusterResult describeClusterResult,
                                     String clusterName,
                                     Session session,
                                     String clusterId) throws InterruptedException, java.util.concurrent.ExecutionException {
        session.run("" +
                "MERGE (c:KafkaCluster { clusterId: \"" + clusterId + "\", " +
                "clusterName: \"" + clusterName + "\" }) " +
                "RETURN c");
        for (Node node : describeClusterResult.nodes().get()) {
            LOGGER.info("Indexing node: {}", node.id());
            session.run("" +
                    "MATCH (c:KafkaCluster) " +
                    "WHERE c.clusterId = \"" + clusterId + "\"" +
                    "MERGE (n:KafkaNode { clusterId: \"" + clusterId + "\", " +
                    "nodeId: " + node.id() + "}) " +
                    "ON CREATE SET " +
                    "n.host = \"" + node.host() + "\", " +
                    "n.port = " + node.port() + " " +
                    "MERGE (n)-[:KAFKA_BROKER_FROM]->(c)" +
                    "RETURN n");

            //if (node.hasRack()) {
                //TODO add racks to graph
            //}
        }
    }

    private static void indexTopics(AdminClient adminClient,
                                    Session session,
                                    String clusterId) throws Exception {
        final Set<String> topicNames = adminClient.listTopics(new ListTopicsOptions().listInternal(true)).names().get();
        for (TopicDescription topic : adminClient.describeTopics(topicNames).all().get().values()) {
            LOGGER.info("Indexing topic: {}", topic.name());
            session.run("" +
                    "MATCH (c:KafkaCluster) " +
                    "WHERE c.clusterId = \"" + clusterId + "\"" +
                    "MERGE (t:KafkaTopic { clusterId: \"" + clusterId + "\", " +
                    "topicName: \"" + topic.name() + "\"}) " +
                    "ON CREATE SET " +
                    "t.isInternal = " + topic.isInternal() + ", " +
                    "t.partitions = " + topic.partitions().size() + ", " +
                    "t.replicationFactor = " + topic.partitions().get(0).replicas().size() + " " +
                    "MERGE (t)-[:KAFKA_TOPIC_FROM]->(c)" +
                    "RETURN t");

            for (TopicPartitionInfo tp : topic.partitions()) {
                session.run("" +
                        "MATCH (t:KafkaTopic) " +
                        "WHERE t.clusterId = \"" + clusterId + "\" " +
                        "AND t.topicName = \"" + topic.name() + "\" " +
                        "MERGE (tp:KafkaTopicPartition { clusterId: \"" + clusterId + "\", " +
                        "topicName: \"" + topic.name() + "\", " +
                        "topicPartition: " + tp.partition() + "}) " +
                        "MERGE (tp)-[:KAFKA_TOPIC_PARTITION_OF]->(t)");
                for (Node replica : tp.replicas()) {
                    session.run("" +
                            "MATCH (tp:KafkaTopicPartition) " +
                            "WHERE tp.clusterId = \"" + clusterId + "\" " +
                            "AND tp.topicName = \"" + topic.name() + "\" " +
                            "AND tp.topicPartition = " + tp.partition() + " " +
                            "MATCH (n:KafkaNode) " +
                            "WHERE n.clusterId = \"" + clusterId + "\" " +
                            "AND n.nodeId = " + replica.id() + " " +
                            "MERGE (n)-[:HOST_KAFKA_REPLICA]->(tp)");
                }
            }
        }
    }
}
