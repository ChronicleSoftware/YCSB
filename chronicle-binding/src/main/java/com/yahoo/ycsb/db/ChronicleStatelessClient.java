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
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.lang.io.serialization.impl.MapMarshaller;
import net.openhft.lang.io.serialization.impl.StringMarshaller;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;


public class ChronicleStatelessClient extends DB {

    private static final String FILE_NAME = "chronicle.file";
    private static final boolean KEY_CHECK = Boolean.getBoolean("key.check");
    private static final AtomicInteger count = new AtomicInteger();
    private static final String HOSTNAME = System.getProperty("server", "localhost");
    private static final int PORT = Integer.getInteger("port", 8076);

    private static ChronicleMap<String, Map<String, String>> serverMap;
    final Set<String> keys = Collections.synchronizedSet(new HashSet<String>());
    private ChronicleMap<String, Map<String, String>> statelessMap;

    public void init() throws DBException {
        synchronized (ChronicleClient.class) {
            Properties props = getProperties();
            try {
                if (serverMap == null) {
                    // server
                    if (HOSTNAME.equals("localhost")) {
                        long recordCount = Long.parseLong(props.getProperty("recordcount", "1000000"));
                        serverMap = startServer(recordCount, props);
                    }
                }
                // stateless client

                long recordCount = Long.parseLong(props.getProperty("recordcount", "1000000"));
                int fieldcount = Integer.parseInt(props.getProperty("fieldcount", "10"));
                int fieldlength = Integer.parseInt(props.getProperty("fieldlength", "100"));
                int entrySize = fieldcount * fieldlength * 12 / 10 + 10;

                statelessMap = ((ChronicleMapBuilder<String, Map<String, String>>)
                        (ChronicleMapBuilder)
                                ChronicleMapBuilder.of(String.class, Map.class))
                        .entries(recordCount)
                        .entrySize(entrySize)
                        .keyMarshaller(new StringMarshaller(0))
                        .putReturnsNull(true)
                        .removeReturnsNull(true)
                        .valueMarshaller(
                                new MapMarshaller<String, String>(new StringMarshaller(128), new StringMarshaller(0)))
                        .statelessClient(new InetSocketAddress(HOSTNAME, PORT))
                        .create();
            } catch (IOException e) {
                throw new DBException(e);
            }
            count.incrementAndGet();
        }
    }

    private static ChronicleMap<String, Map<String, String>> startServer(long recordCount, Properties props) throws IOException {
        String tmp = System.getProperty("java.io.tmpdir");

        int fieldcount = Integer.parseInt(props.getProperty("fieldcount", "10"));
        int fieldlength = Integer.parseInt(props.getProperty("fieldlength", "100"));
        int entrySize = fieldcount * fieldlength * 12 / 10 + 10;

        return ((ChronicleMapBuilder<String, Map<String, String>>)
                (ChronicleMapBuilder)
                        ChronicleMapBuilder.of(String.class, Map.class))
                .entries(recordCount)
                .entrySize(entrySize)
                .keyMarshaller(new StringMarshaller(0))
                .putReturnsNull(true)
                .removeReturnsNull(true)
                .valueMarshaller(
                        new MapMarshaller<String, String>(new StringMarshaller(128), new StringMarshaller(0)))
                .replication((byte) 1, TcpTransportAndNetworkConfig.of(PORT))
                .create();
    }

    //XXX jedis.select(int index) to switch to `table`

    public void cleanup() throws DBException {
        if (count.decrementAndGet() == 0) {
            serverMap.close();
        }
        statelessMap.close();
    }

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

    /**
     * To be run on a server for clients to connect to.
     */
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: " + ChronicleStatelessClient.class.getName() + " recordCount");
            System.exit(1);
        }
        long recordCount = Long.parseLong(args[0]);
        ChronicleMap<String, Map<String, String>> map = startServer(recordCount, System.getProperties());
        System.out.println("server running on port " + PORT);
        System.in.read();
        map.close();
    }
}
