package kafka.tools.internal;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;

import java.util.function.Supplier;

public class Neo4jDriverSupplier implements Supplier<Driver> {
    @Override
    public Driver get() {
        Config config = ConfigFactory.load();

        return GraphDatabase.driver(config.getString("neo4j.uri"),
                AuthTokens.basic(config.getString("neo4j.auth.username"), config.getString("neo4j.auth.password")));
    }
}
