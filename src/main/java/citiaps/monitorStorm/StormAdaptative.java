package citiaps.monitorStorm;

import java.util.List;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import citiaps.monitorStorm.eda.TopologyApp;
import citiaps.monitorStorm.mape.Analyze;
import citiaps.monitorStorm.mape.Execute;
import citiaps.monitorStorm.mape.Monitor;
import citiaps.monitorStorm.mape.Plan;
import citiaps.monitorStorm.util.Config;
import citiaps.monitorStorm.util.Metrics;
import citiaps.monitorStorm.util.NimbusUtils;

public class StormAdaptative {
	private final static Logger logger = LoggerFactory.getLogger(StormAdaptative.class);

	private static String IP_NIMBUS = "127.0.0.1";
	private static int PORT_NIMBUS = 6627;

	private static boolean notAnalyze;

	public static void main(String[] args) {
		String topologyName = args[0];

		IP_NIMBUS = args[1];
		PORT_NIMBUS = Integer.parseInt(args[2]);

		if (args[3] == "stats") {
			notAnalyze = true;
		}

		NimbusUtils nimbusUtils = new NimbusUtils(IP_NIMBUS, PORT_NIMBUS);
		String topologyId = nimbusUtils.searchID(topologyName);

		Config confMape = new Config();
		confMape.readConfig("config/mape.xml");

		Metrics metrics = new Metrics("stats/" + topologyId);

		TopologyApp topologyApp = new TopologyApp(confMape);
		topologyApp.createTopology(IP_NIMBUS, PORT_NIMBUS, topologyId, metrics);

		ScheduledExecutorService mapeService = Executors.newSingleThreadScheduledExecutor();
		mapeService.scheduleAtFixedRate(new MAPE(topologyId, topologyApp, confMape, metrics), 0, 5000,
				TimeUnit.MILLISECONDS);

		metrics.start();
	}

	private static class MAPE extends TimerTask {
		private TopologyApp topologyApp;

		private Metrics metrics;

		private Monitor monitor;
		private Analyze analyze;
		private Plan plan;
		private Execute execute;

		public MAPE(String topologyId, TopologyApp topologyApp, Config confMape, Metrics metrics) {
			this.topologyApp = topologyApp;

			this.metrics = metrics;

			this.monitor = new Monitor(IP_NIMBUS, PORT_NIMBUS, topologyId, this.topologyApp, confMape);
			this.analyze = new Analyze(this.topologyApp, confMape);
			this.plan = new Plan(this.topologyApp, confMape);
			this.execute = new Execute(IP_NIMBUS, PORT_NIMBUS, topologyId, this.topologyApp);
		}

		@Override
		public void run() {
			String status = this.monitor.gettingStats();
			logger.info("{Status=" + status + "}");

			this.metrics.sendStats(this.topologyApp);

			if (notAnalyze) {

				if (status.equals("reactive")) {
					Map<String, String> statusBolts = this.analyze.reactive();
					logger.info("[Reactive] " + statusBolts.toString());
					if (!statusBolts.isEmpty()) {
						List<String> planningBolts = this.plan.planning(statusBolts, false);
						logger.info("[Planning] " + planningBolts.toString());
						if (!planningBolts.isEmpty()) {
							logger.info("[Rebalance Topology]");
							this.execute.rebalanceTopology(planningBolts);
						}
					}

				} else if (status.equals("predictive")) {
					Map<String, String> statusBolts = this.analyze.predictive();
					logger.info("[Predictive] " + statusBolts.toString());
					if (!statusBolts.isEmpty()) {
						List<String> planningBolts = this.plan.planning(statusBolts, true);
						logger.info("[Planning] " + planningBolts.toString());
						if (!planningBolts.isEmpty()) {
							logger.info("[Rebalance Topology]");
							this.execute.rebalanceTopology(planningBolts);
						}
					}
				}

			}
		}
	}
}
