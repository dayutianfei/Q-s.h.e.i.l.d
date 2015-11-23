package cn.dayutianfei.hadoop.metrics;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsSource;

public class MyMetrics implements MetricsSource {

//	public void getMetrics(MetricsBuilder builder, boolean all) {
//		builder.addRecord("myRecord").setContext("myContext")
//				.addGauge("myMetric", "My metrics description", 42);
//	}

	@Override
	public void getMetrics(MetricsCollector arg0, boolean arg1) {
		// TODO Auto-generated method stub
		
	}

}
