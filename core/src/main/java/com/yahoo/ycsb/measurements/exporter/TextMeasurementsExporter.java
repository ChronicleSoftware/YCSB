package com.yahoo.ycsb.measurements.exporter;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

/**
 * Write human readable text. Tries to emulate the previous print report method.
 */
public class TextMeasurementsExporter implements MeasurementsExporter {

    private BufferedWriter bw;

    public TextMeasurementsExporter(OutputStream os) {
        this.bw = new BufferedWriter(new OutputStreamWriter(os));
    }

    public void write(String metric, String measurement, int i) throws IOException {
        bw.write("[" + metric + "], " + measurement + ", " + i);
        bw.newLine();
    }

    public void write(String metric, String measurement, double d) throws IOException {
        bw.write("[" + metric + "], " + measurement + ", ");
        if ((long) d == d)
            bw.write(String.format("%,d", (long) d));
        else
            bw.write(Double.toString(d));
        bw.newLine();
    }

    public void close() throws IOException {
        this.bw.close();
    }

}
