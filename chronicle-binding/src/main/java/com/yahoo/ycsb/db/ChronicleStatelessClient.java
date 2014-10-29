/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Redis client binding for YCSB.
 *
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.hash.TcpReplicationConfig;
import net.openhft.lang.io.serialization.impl.MapMarshaller;
import net.openhft.lang.io.serialization.impl.StringMarshaller;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static net.openhft.chronicle.hash.StatelessBuilder.remoteAddress;

public class ChronicleStatelessClient extends DB {

    private static final boolean KEY_CHECK = Boolean.getBoolean("key.check");
    private ChronicleMap<String, Map<String, String>> statelessMap;
    private static ChronicleMap<String, Map<String, String>> serverMap;
    private static final AtomicInteger count = new AtomicInteger();


    public void init() throws DBException {
        synchronized (ChronicleClient.class) {
            // stateless client
            try {
                statelessMap = ((ChronicleMapBuilder<String, Map<String, String>>)
                        (ChronicleMapBuilder)
                                ChronicleMapBuilder.of(String.class, Map.class))
                        .entrySize(1200)
                        .keyMarshaller(new StringMarshaller(0))
                        .valueMarshaller(
                                new MapMarshaller<String, String>(new StringMarshaller(128), new StringMarshaller(0)))
                        .stateless(remoteAddress(new InetSocketAddress("localhost", 8076)))
                        .create();
            } catch (IOException e) {
                throw new DBException(e);
            }

            count.incrementAndGet();
            if (serverMap != null) return;

            Properties props = getProperties();
            long recordCount = Long.parseLong(props.getProperty("recordcount", "1000000"));


            // server
            {
                try {
                    serverMap = ((ChronicleMapBuilder<String, Map<String, String>>)
                            (ChronicleMapBuilder)
                                    ChronicleMapBuilder.of(String.class, Map.class))
                            .entries(recordCount)
                            .entrySize(1200)
                            .keyMarshaller(new StringMarshaller(0))
                            .putReturnsNull(true)
                            .removeReturnsNull(true)
                            .valueMarshaller(
                                    new MapMarshaller<String, String>(new StringMarshaller(128), new StringMarshaller(0)))
                            .replicators((byte) 1, TcpReplicationConfig.of(8076)).create();
                } catch (IOException e) {
                    throw new DBException(e);
                }

            }

        }
    }

    public void cleanup() throws DBException {
        try {
            if (count.decrementAndGet() == 0) {
                serverMap.close();
            }
            statelessMap.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //XXX jedis.select(int index) to switch to `table`

    @Override
    public int read(String table, String key, Set<String> fields,
                    HashMap<String, ByteIterator> result) {

        Map<String, String> values = statelessMap.get(key);
        if (values == null) {
            if (KEY_CHECK && keys.contains(key))
                throw new AssertionError("Couldn't find a key which should be there " + key);
            return 1;
        }
        if (fields == null) {
            StringByteIterator.putAllAsByteIterators(result, values);
        } else {
            values.keySet().retainAll(fields);
            StringByteIterator.putAllAsByteIterators(result, values);
        }
        return 0;
    }

    final Set<String> keys = Collections.synchronizedSet(new HashSet<String>());

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        statelessMap.put(key, StringByteIterator.getStringMap(values));
        if (KEY_CHECK)
            keys.add(key);
        return 0;
    }

    @Override
    public int delete(String table, String key) {
        if (KEY_CHECK) keys.remove(key);
        if (statelessMap.containsKey(key)) {
            statelessMap.remove(key);
            return 0;
        }
        return 1;
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        if (KEY_CHECK) keys.add(key);
        Map<String, String> values0 = statelessMap.get(key);
        if (values0 == null) {
            values0 = StringByteIterator.getStringMap(values);
        } else {
            values0.putAll(StringByteIterator.getStringMap(values));
        }
        statelessMap.put(key, values0);
        return 0;
    }

    @Override
    public int scan(String table, String startkey, int recordcount,
                    Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        boolean found = false;
        for (String key : statelessMap.keySet()) {
            if (key.equals(startkey))
                found = true;
            if (!found)
                continue;
            if (recordcount-- <= 0)
                return 0;
            HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
            read(table, key, fields, values);
            result.add(values);
        }
        return 0;
    }

}
