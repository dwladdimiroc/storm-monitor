package citiaps.monitorStorm.mape;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import citiaps.monitorStorm.eda.TopologyApp;
import citiaps.monitorStorm.model.MarkovChain;
import citiaps.monitorStorm.util.Config;

public class Analyze {
	private TopologyApp topologyApp;
	private Config confMape;

	public Analyze(TopologyApp topologyApp, Config confMape) {
		this.topologyApp = topologyApp;
		this.confMape = confMape;
	}

	public Map<String, String> reactive() {
		Map<String, String> statusBolts = new HashMap<String, String>();

		for (String id : this.topologyApp.keySet()) {
			double C = this.topologyApp.getStats(id).getLoad(confMape.getSamplesReactive());

			if (C > this.confMape.getUpper()) {
				statusBolts.put(id, "inestable");
			} else if (C < this.confMape.getLower()) {
				if (this.topologyApp.getStats(id).getReplicas() > 1) {
					statusBolts.put(id, "ocioso");
				}
			}

		}

		return statusBolts;
	}

	public Map<String, String> predictive() {
		Map<String, String> statusBolts = new HashMap<String, String>();

		MarkovChain markovChain = new MarkovChain(confMape.getUpper(), confMape.getLower());
		for (String id : this.topologyApp.keySet()) {
			/* Parseo del List a Array */
			Double rho[] = new Double[this.topologyApp.getStats(id).getHistory().size()];
			rho = this.topologyApp.getStats(id).getHistory().toArray(rho);

			/* Cálculo de la predicción por parte de la Cadena de Markov */
			double distEstacionaria[] = markovChain.calculatePrediction(rho, confMape.getSamplesPredictive(),
					confMape.getIterationPredictive());

			/* Análisis estadístico de los resultados de la predicción */
			DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics(distEstacionaria);

			/*
			 * En caso de existir una desviación estándar mayor al umbral, se
			 * tomará como antecedente para la replicación de la historia
			 * analizada.
			 */
			if (descriptiveStatistics.getStandardDeviation() > confMape.getThresholdPredictive()) {
				for (int i = 0; i < distEstacionaria.length; i++) {
					if (distEstacionaria[i] == descriptiveStatistics.getMax()) {
						if (i == 0) {
							statusBolts.put(id, "ocioso");
						} else if (i == 2) {
							statusBolts.put(id, "inestable");
						}
					}
				}
			}
		}

		return statusBolts;

	}

}
