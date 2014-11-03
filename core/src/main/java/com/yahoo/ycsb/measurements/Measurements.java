package com.yahoo.ycsb.measurements;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;

/**
 * Collects latency measurements, and reports them when requested.
 * 
 * @author cooperb
 *
 */
public class Measurements
{
	private static final String MEASUREMENT_TYPE = "measurementtype";

	private static final String MEASUREMENT_TYPE_DEFAULT = "histogram";

	static Measurements singleton=null;
	
	static Properties measurementproperties=null;
	
	public static void setProperties(Properties props)
	{
		measurementproperties=props;
	}

      /**
       * Return the singleton Measurements object.
       */
	public synchronized static Measurements getMeasurements()
	{
		if (singleton==null)
		{
			singleton=new Measurements(measurementproperties);
		}
		return singleton;
	}

    final List<Map<String, OneMeasurement>> allData = Collections.synchronizedList(new ArrayList<Map<String, OneMeasurement>>());
	final ThreadLocal<Map<String,OneMeasurement>> data = new ThreadLocal<Map<String, OneMeasurement>>() {
        @Override
        protected Map<String, OneMeasurement> initialValue() {
            Map<String, OneMeasurement> map = new HashMap<String, OneMeasurement>();
                allData.add(map);
            return map;
        }
    };
	boolean histogram=true;

	private Properties _props;
	
      /**
       * Create a new object with the specified properties.
       */
	public Measurements(Properties props)
	{
		_props=props;
		
		if (_props.getProperty(MEASUREMENT_TYPE, MEASUREMENT_TYPE_DEFAULT).compareTo("histogram")==0)
		{
			histogram=true;
		}
		else
		{
			histogram=false;
		}
	}
	
	OneMeasurement constructOneMeasurement(String name)
	{
		if (histogram)
		{
			return new OneMeasurementHistogram(name,_props);
		}
		else
		{
			return new OneMeasurementTimeSeries(name,_props);
		}
	}

      /**
       * Report a single value of a single metric. E.g. for read latency, operation="READ" and latency is the measured value.
       */
	public void measure(String operation, int latency)
	{
        acquireOneMeasurement(operation).measure(latency);
	}

    private OneMeasurement acquireOneMeasurement(String operation) {
        OneMeasurement oneMeasurement = data.get().get(operation);
        if (oneMeasurement == null) {
            data.get().put(operation, oneMeasurement = constructOneMeasurement(operation));
        }
        return oneMeasurement;
    }

    /**
       * Report a return code for a single DB operaiton.
       */
	public void reportReturnCode(String operation, int code)
	{
        acquireOneMeasurement(operation).reportReturnCode(code);
	}
	
  /**
   * Export the current measurements to a suitable format.
   * 
   * @param exporter Exporter representing the type of format to write to.
   * @throws IOException Thrown if the export failed.
   */
  public void exportMeasurements(MeasurementsExporter exporter) throws IOException
  {
      Map<String, OneMeasurement> data = combineAllData();
    for (OneMeasurement measurement : data.values())
    {
      measurement.exportMeasurements(exporter);
    }
  }

    private Map<String, OneMeasurement> combineAllData() {
        Map<String, OneMeasurement> comb = new HashMap<String, OneMeasurement>();
        synchronized (allData) {
            for (Map<String, OneMeasurement> map : allData) {
                for (Map.Entry<String, OneMeasurement> entry : map.entrySet()) {
                    OneMeasurement om = comb.get(entry.getKey());
                    if (om == null) {
                        comb.put(entry.getKey(), om = entry.getValue().clone());
                    } else {
                        om.merge(entry.getValue());
                    }

                }
            }
        }
        return comb;
    }

    /**
       * Return a one line summary of the measurements.
       */
	public String getSummary()
	{
		String ret="";
        Map<String, OneMeasurement> data = combineAllData();
		for (OneMeasurement m : data.values())
		{
			ret+=m.getSummary()+" ";
		}
		
		return ret;
	}
}
