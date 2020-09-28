package com.github.dwladdimiroc.stormMonitor.eda;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.generated.ComponentPageInfo;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.thrift.TException;
import org.apache.storm.thrift.protocol.TBinaryProtocol;
import org.apache.storm.thrift.transport.TFramedTransport;
import org.apache.storm.thrift.transport.TSocket;
import org.apache.storm.thrift.transport.TTransport;

import com.github.dwladdimiroc.stormMonitor.util.Config;
import com.github.dwladdimiroc.stormMonitor.util.Metrics;
import org.apache.storm.utils.NimbusClient;

public class TopologyApp {
    private Map<String, Stats> topology;
    private Config confMape;

    private InputStats inputStats;

    public TopologyApp(Config confMape) {
        this.topology = new HashMap<String, Stats>();
        this.confMape = confMape;
        this.inputStats = new InputStats();
    }

    public void addBoltTopology(StormTopology stormTopology, Metrics metrics) {
        for (String boltName : stormTopology.get_bolts().keySet()) {
            if (!boltName.equals("__eventlogger") && !boltName.equals("__acker") && !boltName.equals("__system")) {
                int replicas = stormTopology.get_bolts().get(boltName).get_common().get_parallelism_hint();
                addBoltTopology(boltName, replicas);
                metrics.createStatsBolt(boltName);
            }
        }
    }

    public void addSpoutTopology(StormTopology stormTopology, Metrics metrics) {
        for (String spoutName : stormTopology.get_spouts().keySet()) {
            metrics.createStatsSpout(spoutName);
            this.inputStats.setStreamInput(spoutName, Long.valueOf(0));
        }
    }

    public boolean addInputBolts(StormTopology stormTopology, String topologyId, Nimbus.Client client) {
        boolean ok = false;
        while (!ok) {
            boolean cond = true;
            for (String boltName : stormTopology.get_bolts().keySet()) {
                if (topology.containsKey(boltName)) {
                    ComponentPageInfo component = null;
                    try {
                        component = client.getComponentPageInfo(topologyId, boltName, "600", false);
                    } catch (TException e) {
                        e.printStackTrace();
                        break;
                    }

                    if (component.get_gsid_to_input_stats().isEmpty()) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        cond = false;
                        break;
                    }

                    for (GlobalStreamId streamId : component.get_gsid_to_input_stats().keySet()) {
                        topology.get(boltName).getInputs().add(streamId.get_componentId());
                    }
                }
            }
            if (cond) {
                ok = true;
            }
        }

        return true;
    }

    public boolean createTopology(String IP_NIMBUS, int PORT_NIMBUS, String topologyId, Metrics metrics) {
        TTransport tSocket = new TSocket(IP_NIMBUS, PORT_NIMBUS);
        TFramedTransport tTransport = new TFramedTransport(tSocket);
        TBinaryProtocol tBinaryProtocol = new TBinaryProtocol(tTransport);
        Nimbus.Client client = new Nimbus.Client(tBinaryProtocol);

        try {
            tTransport.open();

            StormTopology stormTopology = client.getTopology(topologyId);

            addBoltTopology(stormTopology, metrics);
            addSpoutTopology(stormTopology, metrics);
            addInputBolts(stormTopology, topologyId, client);

            tTransport.close();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    private void addBoltTopology(String bolt, int replicas) {
        if (!topology.containsKey(bolt)) {
            Stats boltStructure = new Stats(confMape.getSamplesPredictive(), confMape.getAlertReactive());
            boltStructure.setId(bolt);
            boltStructure.setReplicas(replicas);

            topology.put(bolt, boltStructure);
        }
    }

    public void sendStats(String id, double timeAvg, long executed, long executedTotal) {
        Stats bolt = this.topology.get(id);

        double timeAvgCurrent = bolt.getTimeAvg() * (double) bolt.getExecuted();
        timeAvg = timeAvg * (double) executed;

        bolt.setExecuted(executed + bolt.getExecuted());
        timeAvgCurrent = (timeAvgCurrent + timeAvg) / (double) bolt.getExecuted();
        bolt.setTimeAvg(timeAvgCurrent);

        bolt.setThroughput(bolt.getExecuted());
        bolt.setExecutedTotal(executedTotal + bolt.getExecutedTotal());
        bolt.setExecutedTotal(executedTotal + bolt.getExecutedTotal());

        this.topology.put(id, bolt);
    }

    public void sendInput(String id, long input) {
        input += this.inputStats.getStreamInput(id);
        this.inputStats.setStreamInput(id, input);
    }

    public void createSample(String id) {
        Stats bolt = this.topology.get(id);

        double executedD = (double) bolt.getExecuted() + (double) this.confMape.getEsync();
        double timeSize = (double) this.confMape.getWindowMonitor() * (double) 1000;
        double replicasD = (double) bolt.getReplicas();

        double capacity = (bolt.getTimeAvg() * executedD) / (timeSize * replicasD);
        bolt.addSample(capacity);

//		bolt.clearTemp();
    }

    public void updateQueue(String id) {
        Stats bolt = this.topology.get(id);
        long queueBolt = bolt.getQueue();
        long input = 0;
        for (String inputId : bolt.getInputs()) {
            if (this.topology.get(inputId) == null) {
                input += this.inputStats.getStreamInput(inputId);
            } else {
                input += this.topology.get(inputId).getExecuted();
            }
        }
        long output = bolt.getExecuted();
        System.out.printf("[Bolt={%s},Queue={%d},Input={%d},Output={%d}]\n", id, bolt.getQueue(), input, output);
        queueBolt += (input - output);
        bolt.setQueue(queueBolt);
    }

    public void clearTemps() {
        for (String id : this.topology.keySet()) {
            Stats bolt = this.topology.get(id);
            bolt.clearTemp();
        }
    }

    public Stats getStats(String id) {
        return this.topology.get(id);
    }

    public String printStats() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("[");
        for (Stats stats : this.topology.values()) {
            stringBuilder = stringBuilder
                    .append("{{ID=" + stats.getId() + "},{History=" + stats.getHistory().toString() + "}}");
        }
        stringBuilder.append("]");

        return new String(stringBuilder);
    }

    public Set<String> keySet() {
        return this.topology.keySet();
    }

    public InputStats getInputStats() {
        return inputStats;
    }

    public void setInputStats(InputStats inputStats) {
        this.inputStats = inputStats;
    }

    @Override
    public String toString() {
        return this.topology.toString();
    }

}
