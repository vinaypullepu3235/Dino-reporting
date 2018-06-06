package com.walmart.xtools.dino.core.utils.kairos.util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class KairosDTO implements Serializable {

    private static final long serialVersionUID = 6519317380250409234L;

    private String name;
    private Map<String, String> tags = new HashMap<String, String>();
    private int ttl;
    private long timestamp;
    private Object value;

    public KairosDTO(String name, Object value, long timestamp, Map<String, String> tags, int ttl) {
        this.name = name;
        this.tags = tags;
        this.ttl = ttl;
        this.timestamp = timestamp;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }


    public int getTtl() {
        return ttl;
    }

    public void setTtl(int ttl) {
        this.ttl = ttl;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }
}
