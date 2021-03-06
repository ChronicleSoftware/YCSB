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
import net.openhft.lang.io.serialization.impl.MapMarshaller;
import net.openhft.lang.io.serialization.impl.StringMarshaller;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ChronicleClient extends DB {

    public static final String FILE_NAME = "chronicle.file";
    private static final boolean KEY_CHECK = Boolean.getBoolean("key.check");
    private static final AtomicInteger count = new AtomicInteger();
    private static ChronicleMap<String, Map<String, String>> map;
    final Set<String> keys = Collections.synchronizedSet(new HashSet<String>());

    public void init() throws DBException {
        synchronized (ChronicleClient.class) {
            count.incrementAndGet();
            if (map != null) return;

            Properties props = getProperties();
            long recordCount = Long.parseLong(props.getProperty("recordcount", "1000000"));
            int fieldcount = Integer.parseInt(props.getProperty("fieldcount", "10"));
            int fieldlength = Integer.parseInt(props.getProperty("fieldlength", "100"));
            int entrySize = fieldcount * fieldlength * 12 / 10 + 10;
            String tmp = System.getProperty("java.io.tmpdir");
            String filename = props.getProperty(FILE_NAME, tmp + "/chronicle-" + recordCount + ".ycsb");
            try {
                map = ChronicleMapBuilder.of(String.class, (Class<Map<String, String>>) (Class) Map.class)
                        .entries(recordCount)
                        .averageValueSize(entrySize)
                        .keyMarshaller(new StringMarshaller(0))
                        .putReturnsNull(true)
                        .removeReturnsNull(true)
                        .valueMarshaller(
                                MapMarshaller.of(new StringMarshaller(128), new StringMarshaller(0)))
                        .createPersistedTo(new File(filename));
            } catch (IOException e) {
                throw new DBException(e);
            }
        }
    }

    public void cleanup() throws DBException {
        if (count.decrementAndGet() == 0)
            map.close();
    }

    @Override
    public int read(String table, String key, Set<String> fields,
                    HashMap<String, ByteIterator> result) {

        Map<String, String> values = map.get(key);
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

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        map.put(key, StringByteIterator.getStringMap(values));
        if (KEY_CHECK)
            keys.add(key);
        return 0;
    }

    @Override
    public int delete(String table, String key) {
        if (KEY_CHECK) keys.remove(key);
        if (map.containsKey(key)) {
            map.remove(key);
            return 0;
        }
        return 1;
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        if (KEY_CHECK) keys.add(key);
        Map<String, String> values0 = map.get(key);
        if (values0 == null) {
            values0 = StringByteIterator.getStringMap(values);
        } else {
            values0.putAll(StringByteIterator.getStringMap(values));
        }
        map.put(key, values0);
        return 0;
    }

    @Override
    public int scan(String table, String startkey, int recordcount,
                    Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        boolean found = false;
        for (String key : map.keySet()) {
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
