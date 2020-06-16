package com.github.dwladdimiroc.stormMonitor.mape;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import com.github.dwladdimiroc.stormMonitor.eda.TopologyApp;
import com.github.dwladdimiroc.stormMonitor.util.Config;

public class Plan {
	private TopologyApp topologyApp;
	private Config confMape;

	public Plan(TopologyApp topologyApp, Config confMape) {
		this.topologyApp = topologyApp;
		this.confMape = confMape;
	}

	public List<String> planning(Map<String, String> statusBolts, boolean predictive) {
		List<String> planningBolts;

		if (!predictive) {
			planningBolts = planAboutReactive(statusBolts);
		} else {
			planningBolts = planAboutPredictive(statusBolts);
		}

		return planningBolts;
	}

	private List<String> planAboutReactive(Map<String, String> statusBolts) {
		List<String> planningBolts = new ArrayList<String>();

		for (String bolt : this.topologyApp.keySet()) {
			if (statusBolts.get(bolt).equals("ocioso")) {
				this.topologyApp.getStats(bolt).getMarkMap().add(-1);
			} else if (statusBolts.get(bolt).equals("inestable")) {
				this.topologyApp.getStats(bolt).getMarkMap().add(1);
			} else {
				this.topologyApp.getStats(bolt).getMarkMap().add(0);
			}

			int value = containsCondition(this.topologyApp.getStats(bolt).getMarkMap());
			if (value != 0) {
				this.topologyApp.getStats(bolt).getMarkMap().clear();
				int replicas = this.confMape.getReplicaReactive() * value;
				replicas = this.topologyApp.getStats(bolt).getReplicas() + replicas;
				if (replicas < 1) {
					replicas = 1;
				}
				this.topologyApp.getStats(bolt).setReplicas(replicas);
				planningBolts.add(bolt);
			}
		}

		return planningBolts;
	}

	private List<String> planAboutPredictive(Map<String, String> statusBolts) {
		List<String> planningBolts = new ArrayList<String>();

		for (String bolt : this.topologyApp.keySet()) {
			if (statusBolts.get(bolt).equals("ocioso")) {
				int replicas = this.confMape.getReplicaReactive();
				replicas = this.topologyApp.getStats(bolt).getReplicas() - replicas;
				if (replicas < 1) {
					replicas = 1;
				}
				this.topologyApp.getStats(bolt).setReplicas(replicas);
				planningBolts.add(bolt);
			} else if (statusBolts.get(bolt).equals("inestable")) {
				int replicas = this.confMape.getReplicaReactive();
				replicas = this.topologyApp.getStats(bolt).getReplicas() + replicas;
				this.topologyApp.getStats(bolt).setReplicas(replicas);
				planningBolts.add(bolt);
			}
		}

		return planningBolts;
	}

	private int containsCondition(Queue<Integer> queue) {
		int value;

		value = 1;
		if (countMark(queue, value)) {
			return value;
		}

		value = -1;
		if (countMark(queue, value)) {
			return value;
		}

		value = 0;
		return value;

	}

	private boolean countMark(Queue<Integer> queue, int value) {
		/* Contador */
		int cont = 0;
		for (int index : queue) {
			/* Si existe análisis de réplica */
			if (index == value) {
				cont++;
				if (cont == this.confMape.getAlertReactive())
					return true;
			}
		}

		return false;
	}
}