package kafka.tools;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaTopologyNeo4jConnector {
    static final Logger LOGGER = LoggerFactory.getLogger(KafkaTopologyNeo4jConnector.class);

    public static void main(String[] args) throws Exception {
        Config config = ConfigFactory.load();

        Properties adminConfig = new Properties();
        adminConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrap-servers"));

        AdminClient adminClient = AdminClient.create(adminConfig);

        DescribeClusterResult describeClusterResult =
                adminClient.describeCluster(new DescribeClusterOptions());

        Driver driver = GraphDatabase.driver(config.getString("neo4j.uri"),
                AuthTokens.basic(config.getString("neo4j.auth.username"), config.getString("neo4j.auth.password")));


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

            if (node.hasRack()) {
                //TODO add racks to graph
            }
        }
    }

    private static void indexTopics(AdminClient adminClient,
                                    Session session,
                                    String clusterId) throws Exception {
        for (TopicDescription topic : adminClient.describeTopics(adminClient.listTopics().names().get()).all().get().values()) {
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
