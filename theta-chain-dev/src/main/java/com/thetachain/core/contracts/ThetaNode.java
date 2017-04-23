package com.thetachain.core.contracts;

import java.util.HashMap;
import java.util.Map;

public final class ThetaNode {
    private String nodeId;
    private Map<String, String> attributes = new HashMap<>();

    public ThetaNode(String nodeId, String... attrs) {
	if (attrs.length % 2 != 0) {
	    throw new IllegalArgumentException("Need even no of args, got " + attrs.length);
	}
	for (int i = 0; i < attrs.length; i = i + 2) {
	    attributes.put(attrs[i], attrs[i + 1]);
	}
    }

    public final String getNodeId() {
	return nodeId;
    }

    public final String getAttribute(String attrName) {
	return attributes.get(attrName);
    }
}
