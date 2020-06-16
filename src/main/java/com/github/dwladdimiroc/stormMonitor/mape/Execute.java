package com.github.dwladdimiroc.stormMonitor.mape;

import java.util.List;

import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.RebalanceOptions;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.thrift.protocol.TBinaryProtocol;
import org.apache.storm.thrift.transport.TFramedTransport;
import org.apache.storm.thrift.transport.TSocket;
import org.apache.storm.thrift.transport.TTransport;

import com.github.dwladdimiroc.stormMonitor.eda.TopologyApp;

public class Execute {
	private String IP_NIMBUS;
	private int PORT_NIMBUS;

	private String idApp;
	private TopologyApp topologyApp;

	public Execute(String IP_NIMBUS, int PORT_NIMBUS, String topologyId, TopologyApp topologyApp) {
		this.IP_NIMBUS = IP_NIMBUS;
		this.PORT_NIMBUS = PORT_NIMBUS;
		this.idApp = topologyId;
		this.topologyApp = topologyApp;
	}

	public void rebalanceTopology(List<String> planningBolts) {
		TTransport tsocket = new TSocket(IP_NIMBUS, PORT_NIMBUS);
		TFramedTransport tTransport = new TFramedTransport(tsocket);
		TBinaryProtocol tBinaryProtocol = new TBinaryProtocol(tTransport);
		Nimbus.Client client = new Nimbus.Client(tBinaryProtocol);
		String topologyId = this.idApp;

		try {
			tTransport.open();

			RebalanceOptions rebalanceOptions = new RebalanceOptions();
			rebalanceOptions.set_wait_secs(0);
			for (String bolt : planningBolts) {
				int replicas = this.topologyApp.getStats(bolt).getReplicas();
				rebalanceOptions.put_to_num_executors(bolt, replicas);
			}

			TopologyInfo topologyInfo = client.getTopologyInfo(topologyId);
			client.rebalance(topologyInfo.get_name(), rebalanceOptions);

			tTransport.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
