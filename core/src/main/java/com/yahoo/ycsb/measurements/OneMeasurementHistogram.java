package com.yahoo.ycsb.measurements;

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


/**
 * Take measurements and maintain a histogram of a given metric, such as READ LATENCY.
 *
 * @author cooperb
 */
public class OneMeasurementHistogram extends OneMeasurement {
    public static final String BUCKETS = "histogram.buckets";
    public static final String BUCKETS_DEFAULT = "100000";

    int _buckets;
    int[] histogram;
    int histogramoverflow;
    int operations;
    long totallatency;

    //keep a windowed version of these stats for printing status
    int windowoperations;
    long windowtotallatency;

    int min;
    int max;
    HashMap<Integer, int[]> returncodes;

    public OneMeasurementHistogram(String name, Properties props) {
        super(name);
        _buckets = Integer.parseInt(props.getProperty(BUCKETS, BUCKETS_DEFAULT));
        histogram = new int[_buckets];
        histogramoverflow = 0;
        operations = 0;
        totallatency = 0;
        windowoperations = 0;
        windowtotallatency = 0;
        min = -1;
        max = -1;
        returncodes = new HashMap<Integer, int[]>();
    }

    @Override
    public synchronized void merge(OneMeasurement value) {
        OneMeasurementHistogram omh = (OneMeasurementHistogram) value;
        assert _buckets == omh._buckets;
        for (int i = 0; i < _buckets; i++)
            histogram[i] += omh.histogram[i];
        histogramoverflow += omh.histogramoverflow;
        operations += omh.operations;
        totallatency += omh.totallatency;
        windowoperations += omh.windowoperations;
        windowtotallatency += omh.windowtotallatency;
        if (min > omh.min) min = omh.min;
        if (max < omh.max) max = omh.max;
        for (Map.Entry<Integer, int[]> entry : omh.returncodes.entrySet()) {
            Integer key = entry.getKey();
            int[] values = entry.getValue();
            int[] ints = returncodes.get(key);
            if (ints == null) {
                returncodes.put(key, values.clone());
            } else {
                for (int i = 0; i < ints.length; i++)
                    ints[i] += values[i];
            }

        }
    }

    /* (non-Javadoc)
         * @see com.yahoo.ycsb.OneMeasurement#reportReturnCode(int)
         */
    public synchronized void reportReturnCode(int code) {
        Integer Icode = code;
        if (!returncodes.containsKey(Icode)) {
            int[] val = new int[1];
            val[0] = 0;
            returncodes.put(Icode, val);
        }
        returncodes.get(Icode)[0]++;
    }


    /* (non-Javadoc)
     * @see com.yahoo.ycsb.OneMeasurement#measure(int)
     */
    public synchronized void measure(int latency) {
        if (latency >= _buckets) {
            histogramoverflow++;
        } else {
            histogram[latency]++;
        }
        operations++;
        totallatency += latency;
        windowoperations++;
        windowtotallatency += latency;

        if ((min < 0) || (latency < min)) {
            min = latency;
        }

        if ((max < 0) || (latency > max)) {
            max = latency;
        }
    }


    @Override
    public void exportMeasurements(MeasurementsExporter exporter) throws IOException {
        exporter.write(getName(), "Operations", operations);
        exporter.write(getName(), "AverageLatency(us)", 100L * totallatency / operations / 100.0);
        exporter.write(getName(), "MinLatency(us)", min);
        exporter.write(getName(), "MaxLatency(us)", max);

        int opcounter = 0;
        boolean done95th = false;
        for (int i = 0; i < _buckets; i++) {
            opcounter += histogram[i];
            if ((double) opcounter / operations >= 0.99) {
                exporter.write(getName(), "99thPercentileLatency(ms)", i / 1e3);
                break;
            } else if ((!done95th) && (double) opcounter / operations >= 0.95) {
                exporter.write(getName(), "95thPercentileLatency(ms)", i / 1e3);
                done95th = true;
            }
        }

        for (Integer I : returncodes.keySet()) {
            int[] val = returncodes.get(I);
            exporter.write(getName(), "Return=" + I, val[0]);
        }

/*    for (int i=0; i<_buckets; i++)
    {
      exporter.write(getName(), Integer.toString(i), histogram[i]);
    }
    exporter.write(getName(), ">"+_buckets, histogramoverflow);
    */
    }

    @Override
    public String getSummary() {
        if (windowoperations == 0) {
            return "";
        }
        DecimalFormat d = new DecimalFormat("#.##");
        double report = ((double) windowtotallatency) / ((double) windowoperations);
        windowtotallatency = 0;
        windowoperations = 0;
        return "[" + getName() + " AverageLatency(us)=" + d.format(report) + "]";
    }

}
