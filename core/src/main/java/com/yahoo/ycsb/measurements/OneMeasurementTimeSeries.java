package com.yahoo.ycsb.measurements;

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Properties;
import java.util.Vector;

class SeriesUnit {
    /**
     * @param time
     * @param average
     */
    public SeriesUnit(long time, double average) {
        this.time = time;
        this.average = average;
    }

    public long time;
    public double average;
}

/**
 * A time series measurement of a metric, such as READ LATENCY.
 */
public class OneMeasurementTimeSeries extends OneMeasurement {
    /**
     * Granularity for time series; measurements will be averaged in chunks of this granularity. Units are milliseconds.
     */
    public static final String GRANULARITY = "timeseries.granularity";

    public static final String GRANULARITY_DEFAULT = "1000";

    int _granularity;
    Vector<SeriesUnit> _measurements;

    long start = -1;
    long currentunit = -1;
    int count = 0;
    int sum = 0;
    int operations = 0;
    long totallatency = 0;

    //keep a windowed version of these stats for printing status
    int windowoperations = 0;
    long windowtotallatency = 0;

    int min = -1;
    int max = -1;

    private HashMap<Integer, int[]> returncodes;

    public OneMeasurementTimeSeries(String name, Properties props) {
        super(name);
        _granularity = Integer.parseInt(props.getProperty(GRANULARITY, GRANULARITY_DEFAULT));
        _measurements = new Vector<SeriesUnit>();
        returncodes = new HashMap<Integer, int[]>();
    }

    void checkEndOfUnit(boolean forceend) {
        long now = System.currentTimeMillis();

        if (start < 0) {
            currentunit = 0;
            start = now;
        }

        long unit = ((now - start) / _granularity) * _granularity;

        if ((unit > currentunit) || (forceend)) {
            double avg = ((double) sum) / ((double) count);
            _measurements.add(new SeriesUnit(currentunit, avg));

            currentunit = unit;

            count = 0;
            sum = 0;
        }
    }

    @Override
    public void measure(int latency) {
        checkEndOfUnit(false);

        count++;
        sum += latency;
        totallatency += latency;
        operations++;
        windowoperations++;
        windowtotallatency += latency;

        if (latency > max) {
            max = latency;
        }

        if ((latency < min) || (min < 0)) {
            min = latency;
        }
    }


    @Override
    public void exportMeasurements(MeasurementsExporter exporter) throws IOException {
        checkEndOfUnit(true);

        exporter.write(getName(), "Operations", operations);
        exporter.write(getName(), "AverageLatency(us)", 100L * totallatency / operations / 100.0);
        exporter.write(getName(), "MinLatency(us)", min);
        exporter.write(getName(), "MaxLatency(us)", max);

        //TODO: 95th and 99th percentile latency

        for (Integer I : returncodes.keySet()) {
            int[] val = returncodes.get(I);
            exporter.write(getName(), "Return=" + I, val[0]);
        }

        for (SeriesUnit unit : _measurements) {
            exporter.write(getName(), Long.toString(unit.time), unit.average);
        }
    }

    @Override
    public void reportReturnCode(int code) {
        Integer Icode = code;
        if (!returncodes.containsKey(Icode)) {
            int[] val = new int[1];
            val[0] = 0;
            returncodes.put(Icode, val);
        }
        returncodes.get(Icode)[0]++;

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

    @Override
    public void merge(OneMeasurement value) {
        throw new UnsupportedOperationException();
    }
}
