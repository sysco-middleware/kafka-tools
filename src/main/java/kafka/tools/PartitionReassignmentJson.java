package kafka.tools;

import java.util.ArrayList;
import java.util.List;

public class PartitionReassignmentJson {
    final int version;
    final List<Partition> partitions;

    PartitionReassignmentJson(int version, List<Partition> partitions) {
        this.version = version;
        this.partitions = partitions;
    }

    static Builder newBuilder() {
        return new Builder();
    }

    static class Partition {
        final String topic;
        final Integer partition;
        final List<Integer> replicas;

        Partition(String topic, Integer partition, List<Integer> replicas) {
            this.topic = topic;
            this.partition = partition;
            this.replicas = replicas;
        }
    }

    public static class Builder {
        List<Partition> partitions = new ArrayList<>();
        int version = 1;

        public Builder addPartition(Partition partition) {
            this.partitions.add(partition);
            return this;
        }

        public PartitionReassignmentJson build() {
            return new PartitionReassignmentJson(version, partitions);
        }
    }
}
