package com.github.dwladdimiroc.stormMonitor;

import java.util.List;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dwladdimiroc.stormMonitor.eda.TopologyApp;
import com.github.dwladdimiroc.stormMonitor.mape.Analyze;
import com.github.dwladdimiroc.stormMonitor.mape.Execute;
import com.github.dwladdimiroc.stormMonitor.mape.Monitor;
import com.github.dwladdimiroc.stormMonitor.mape.Plan;
import com.github.dwladdimiroc.stormMonitor.util.Config;
import com.github.dwladdimiroc.stormMonitor.util.Metrics;
import com.github.dwladdimiroc.stormMonitor.util.NimbusUtils;

public class StormAdaptative {
    private final static Logger logger = LoggerFactory.getLogger(StormAdaptative.class);

    private static String IP_NIMBUS = "127.0.0.1";
    private static int PORT_NIMBUS = 6627;

    private static boolean notAnalyze;

    public static void main(String[] args) {
        String topologyName = args[0];

        IP_NIMBUS = args[1];
        PORT_NIMBUS = Integer.parseInt(args[2]);

        if (args.length > 3 && args[3].equals("stats")) {
            notAnalyze = true;
        }

        NimbusUtils nimbusUtils = new NimbusUtils(IP_NIMBUS, PORT_NIMBUS);
        String topologyId = nimbusUtils.searchID(topologyName);

        Config confMape = new Config();
        confMape.readConfig("conf/mape.xml");

        Metrics metrics = new Metrics("stats/" + topologyId, confMape);

        TopologyApp topologyApp = new TopologyApp(confMape);
        boolean ok = topologyApp.createTopology(IP_NIMBUS, PORT_NIMBUS, topologyId, metrics);

        if (ok) {
            ScheduledExecutorService mapeService = Executors.newSingleThreadScheduledExecutor();
            mapeService.scheduleAtFixedRate(new MAPE(topologyId, topologyApp, confMape, metrics), 0, confMape.getWindowMonitor(),
                    TimeUnit.SECONDS);

            metrics.start();
        }
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

            if (!status.equals("waiting...")) {
                this.metrics.sendStats(this.topologyApp);
                this.topologyApp.clearTemps();

                if (!notAnalyze) {
                    if (status.equals("reactive")) {
                        Map<String, String> statusBolts = this.analyze.reactive();
                        logger.info("[Reactive] " + statusBolts.toString());
                        if (!statusBolts.isEmpty()) {
                            List<String> planningBolts = this.plan.planning(statusBolts, false);
                            logger.info("[Planning] " + planningBolts.toString());
                            if (!planningBolts.isEmpty()) {
                                logger.info("[Rebalanced Topology]");
                                this.execute.rebalancedTopology(planningBolts);
                            }
                        }

                    } else if (status.equals("predictive")) {
                        Map<String, String> statusBolts = this.analyze.predictive();
                        logger.info("[Predictive] " + statusBolts.toString());
                        if (!statusBolts.isEmpty()) {
                            List<String> planningBolts = this.plan.planning(statusBolts, true);
                            logger.info("[Planning] " + planningBolts.toString());
                            if (!planningBolts.isEmpty()) {
                                logger.info("[Rebalanced Topology]");
                                this.execute.rebalancedTopology(planningBolts);
                            }
                        }
                    }

                }
            }
        }
    }
}
