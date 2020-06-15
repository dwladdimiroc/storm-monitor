package citiaps.monitorStorm.eda;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.thrift.protocol.TBinaryProtocol;
import org.apache.storm.thrift.transport.TFramedTransport;
import org.apache.storm.thrift.transport.TSocket;
import org.apache.storm.thrift.transport.TTransport;

import citiaps.monitorStorm.util.Config;
import citiaps.monitorStorm.util.Metrics;

public class TopologyApp {
	private Map<String, Stats> topology;
	private Config confMape;

	private InputStats inputStats;

	public TopologyApp(Config confMape) {
		this.topology = new HashMap<String, Stats>();
		this.confMape = confMape;
		this.inputStats = new InputStats();
	}

	public void createTopology(String IP_NIMBUS, int PORT_NIMBUS, String topologyId, Metrics metrics) {
		TTransport tsocket = new TSocket(IP_NIMBUS, PORT_NIMBUS);
		TFramedTransport tTransport = new TFramedTransport(tsocket);
		TBinaryProtocol tBinaryProtocol = new TBinaryProtocol(tTransport);
		Nimbus.Client client = new Nimbus.Client(tBinaryProtocol);

		try {
			tTransport.open();

			StormTopology stormTopology = client.getTopology(topologyId);

			for (String boltName : stormTopology.get_bolts().keySet()) {
				if (!boltName.equals("__eventlogger") && !boltName.equals("__acker") && !boltName.equals("__system")) {
					int replicas = stormTopology.get_bolts().get(boltName).get_common().get_parallelism_hint();
					addBoltTopology(boltName, replicas);
					metrics.createStatsBolt(boltName);
				}
			}

			for (String spoutName : stormTopology.get_spouts().keySet()) {
				metrics.createStatsSpout(spoutName);
				this.inputStats.setStreamInput(spoutName, Long.valueOf(0));
			}

			tTransport.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

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

		bolt.clearTemp();
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
