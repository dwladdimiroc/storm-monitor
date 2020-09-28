package com.github.dwladdimiroc.stormMonitor.util;

import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.thrift.protocol.TBinaryProtocol;
import org.apache.storm.thrift.transport.TFramedTransport;
import org.apache.storm.thrift.transport.TSocket;
import org.apache.storm.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NimbusUtils {
	private final static Logger logger = LoggerFactory.getLogger(NimbusUtils.class);

	private String IP_NIMBUS;
	private int PORT_NIMBUS;

	public NimbusUtils(String IP_NIMBUS, int PORT_NIMBUS) {
		this.IP_NIMBUS = IP_NIMBUS;
		this.PORT_NIMBUS = PORT_NIMBUS;
	}

	public String searchID(String name) {
		String id = "default";

		TTransport tSocket = new TSocket(IP_NIMBUS, PORT_NIMBUS);
		TFramedTransport tTransport = new TFramedTransport(tSocket);
		TBinaryProtocol tBinaryProtocol = new TBinaryProtocol(tTransport);
		Nimbus.Client client = new Nimbus.Client(tBinaryProtocol);

		try {
			tTransport.open();

			for (TopologySummary topologies : client.getClusterInfo().get_topologies()) {
				if (topologies.get_id().contains(name)) {
					id = topologies.get_id();
				}
			}

			tTransport.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

		return id;
	}
}
