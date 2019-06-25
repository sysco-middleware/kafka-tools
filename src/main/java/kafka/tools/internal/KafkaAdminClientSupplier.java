package kafka.tools.internal;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.util.Properties;

public class KafkaAdminClientSupplier implements java.util.function.Supplier<AdminClient> {
    final Config config = ConfigFactory.load();

    public static AdminClient create() {
        return new KafkaAdminClientSupplier().get();
    }

    @Override
    public AdminClient get() {
        Properties adminConfig = new Properties();
        adminConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrap-servers"));

        return AdminClient.create(adminConfig);
    }
}
