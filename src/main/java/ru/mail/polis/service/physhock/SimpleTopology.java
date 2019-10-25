package ru.mail.polis.service.physhock;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Set;

/**
 * Simple way to handle sharding.
 */
public class SimpleTopology implements Topology<String> {

    private final String thisNode;
    private final String[] allNodes;

    /**
     * Class for serving sharding features.
     *
     * @param thisNode current node
     * @param allNodes list of all nodes
     */
    public SimpleTopology(final String thisNode, final Set<String> allNodes) {
        this.thisNode = thisNode;
        this.allNodes = new String[allNodes.size()];
        allNodes.toArray(this.allNodes);
        Arrays.sort(this.allNodes);
    }

    @Override
    public boolean isMe(final String node) {
        return node.equals(thisNode);
    }

    @Override
    public String calculateFor(final ByteBuffer key) {
        final int hash = key.hashCode();
        final int node = (hash & Integer.MAX_VALUE) % allNodes.length;
        return allNodes[node];
    }

    @Override
    public Set<String> all() {
        return Set.of(allNodes);
    }
}
