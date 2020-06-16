package com.github.dwladdimiroc.stormMonitor.mape;

import java.util.List;

import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.thrift.protocol.TBinaryProtocol;
import org.apache.storm.thrift.transport.TFramedTransport;
import org.apache.storm.thrift.transport.TSocket;
import org.apache.storm.thrift.transport.TTransport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dwladdimiroc.stormMonitor.eda.TopologyApp;
import com.github.dwladdimiroc.stormMonitor.util.Config;

public class Monitor {
	private final static Logger logger = LoggerFactory.getLogger(Monitor.class);
	private final boolean showLogger = false;

	private String IP_NIMBUS;
	private int PORT_NIMBUS;

	private String idApp;
	private TopologyApp topologyApp;

	private Config confMape;

	private int countAnalyze;

	public Monitor() {
		this.IP_NIMBUS = "localhost";
		this.PORT_NIMBUS = 6627;

		this.idApp = "default";
		this.topologyApp = new TopologyApp(confMape);

		this.countAnalyze = 0;
	}

	public Monitor(String IP_NIMBUS, int PORT_NIMBUS, String idApp, TopologyApp topologyApp, Config confMape) {
		this.IP_NIMBUS = IP_NIMBUS;
		this.PORT_NIMBUS = PORT_NIMBUS;

		this.idApp = idApp;
		this.topologyApp = topologyApp;

		this.confMape = confMape;

		this.countAnalyze = 0;
	}

	public String gettingStats() {
		TTransport tsocket = new TSocket(IP_NIMBUS, PORT_NIMBUS);
		TFramedTransport tTransport = new TFramedTransport(tsocket);
		TBinaryProtocol tBinaryProtocol = new TBinaryProtocol(tTransport);
		Nimbus.Client client = new Nimbus.Client(tBinaryProtocol);
		String topologyId = this.idApp;

		try {
			tTransport.open();

			TopologyInfo topologyInfo = client.getTopologyInfo(topologyId);
			List<ExecutorSummary> executorSummaries = topologyInfo.get_executors();
			for (ExecutorSummary executorSummary : executorSummaries) {
				String id = executorSummary.get_component_id();
				if (!id.equals("__acker") && !id.equals("__eventlogger")) {
					ExecutorStats executorStats = executorSummary.get_stats();
					if (executorStats.get_specific().is_set_bolt()) {

						if (executorStats.get_specific().get_bolt().get_executed().get("600") != null
								&& executorStats.get_specific().get_bolt().get_execute_ms_avg().get("600") != null
								&& executorStats.get_specific().get_bolt().get_execute_ms_avg()
										.get(":all-time") != null) {

							double timeAvg = 0;
							long executed = 0;

							long executedTotal = 0;

							for (GlobalStreamId key : executorStats.get_specific().get_bolt().get_executed().get("600")
									.keySet()) {

								executed += executorStats.get_specific().get_bolt().get_executed().get("600").get(key);
								timeAvg += executorStats.get_specific().get_bolt().get_execute_ms_avg().get("600")
										.get(key) * (double) executed;

								executedTotal += executorStats.get_specific().get_bolt().get_executed().get(":all-time")
										.get(key);

								if (showLogger)
									logger.info("[Key={}],[Bolt={}],[Executed={}],[TimeAvg={}]", key, id, executed,
											timeAvg);

							}

							timeAvg /= executed;

							this.topologyApp.sendStats(id, timeAvg, executed, executedTotal);
						}
					} else if (executorStats.get_specific().is_set_spout()) {
						if (executorStats.get_transferred().get("600") != null) {
							long input = executorStats.get_transferred().get("600").get("default");
							this.topologyApp.sendInput(id, input);

							if (showLogger)
								logger.info("[Spout={}],[Input={}]", id, input);
						}
					}

				}
			}

			for (String id : this.topologyApp.keySet()) {
				this.topologyApp.createSample(id);
			}

			if (showLogger)
				logger.info(this.topologyApp.printStats());

		} catch (Exception e) {
			e.printStackTrace();
			return "waiting...";
		} finally {
			tTransport.close();
		}

		countAnalyze++;
		if (countAnalyze % confMape.getSamplesPredictive() == 0) {
			countAnalyze = 0;
			return "predictive";
		} else if (countAnalyze % confMape.getSamplesReactive() == 0) {
			return "reactive";
		} else {
			return "nothing";
		}
	}
}