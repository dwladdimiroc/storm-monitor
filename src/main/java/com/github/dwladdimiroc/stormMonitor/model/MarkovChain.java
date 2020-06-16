package com.github.dwladdimiroc.stormMonitor.model;

import java.text.DecimalFormat;

public class MarkovChain {
	private double transitionMatrix[][];
	private double prediction[];

	private double upper;
	private double lower;

	/**
	 * Inicialización de la matriz de transición y el arreglo para la
	 * distribución estacionaria
	 */
	public MarkovChain(double upper, double lower) {
		setTransitionMatrix(new double[3][3]);
		setPrediction(new double[3]);

		this.upper = upper;
		this.lower = lower;
	}

	/**
	 * Cálculo para la distribución estacionaria, para esto se analizará la tasa
	 * de procesamiento respecto a un período t y t+1, de esta manera se
	 * verifica a que estado a variado. Por lo que se consideraron tres posibles
	 * estados en la Cadena de Markov: Ocioso, Estable e Inestable. Ocioso se
	 * refiere a cuando existen una cantidad de servicios que poseen un alto
	 * tiempo de ocio. Estable es cuando se posee con un buen número de
	 * procesamiento. E inestable cuando existe un tasa de procesamiento menor
	 * que la tasa de llegada, por lo que generará colas en el sistema.
	 * 
	 * @param rho
	 *            Tasa de procesamiento del sistema en un
	 * @param n
	 *            Número muestras que se poseen
	 * @param iteration
	 *            Número de iteraciones en para calcular la distribución
	 *            estacionaria de la cadena de Markov
	 * @return Distribución estacionaria de la Cadena de Markov, generada con
	 *         las muestras de los datos de entrada
	 */

	public double[] calculatePrediction(Double[] rho, int n, int iteration) {
		/*
		 * En esta fase se analizará la probabilidad de las distintas
		 * transiciones de un estado a otro. Para esto se ha dispuesto de un
		 * contador por cada una de las distintas transiciones, para luego
		 * normalizar la matriz de transición, de esta manera quedarán datos
		 * entre [0-1], dando así la probabilidad de cambiar de un estado a
		 * otro.
		 */
		if (n > rho.length) {
			n = rho.length;
		}

		int cont[] = new int[3];
		for (int i = 0; i < n - 1; i++) {

			/*
			 * En este caso se analizó si en cierto período de tiempo se mantuvo
			 * el sistema en el estado ocioso.
			 */
			if ((rho[i] < lower) && (rho[i + 1] < lower)) {
				transitionMatrix[0][0]++;
				cont[0]++;
			}
			// Que haya pasado de un estado ocioso a uno estable
			else if ((rho[i] < lower) && (rho[i + 1] >= lower) && (rho[i + 1] <= upper)) {
				transitionMatrix[0][1]++;
				cont[0]++;
			}
			// De un estado ocioso a uno inestable
			else if ((rho[i] < lower) && (rho[i + 1] > upper)) {
				transitionMatrix[0][2]++;
				cont[0]++;
			}
			// De un estado estable a uno ocioso
			else if ((rho[i] >= lower) && (rho[i] <= upper) && (rho[i + 1] < lower)) {
				transitionMatrix[1][0]++;
				cont[1]++;
			}
			// De que se mantenga en el sistema estable
			else if ((rho[i] >= lower) && (rho[i] <= upper) && (rho[i + 1] >= lower) && (rho[i + 1] <= upper)) {
				transitionMatrix[1][1]++;
				cont[1]++;
			}
			// De un estado estable a uno inestable
			else if ((rho[i] >= lower) && (rho[i] <= upper) && (rho[i + 1] > upper)) {
				transitionMatrix[1][2]++;
				cont[1]++;
			}
			// De un estado inestable a uno ocioso
			else if ((rho[i] > upper) && (rho[i + 1] < lower)) {
				transitionMatrix[2][0]++;
				cont[2]++;
			}
			// De un estado inestable a uno estable
			else if ((rho[i] > upper) && (rho[i + 1] >= lower) && (rho[i + 1] <= upper)) {
				transitionMatrix[2][1]++;
				cont[2]++;
			}
			// Que se mantenga inestable el sistema
			else if ((rho[i] > upper) && (rho[i + 1] > upper)) {
				transitionMatrix[2][2]++;
				cont[2]++;
			}
		}

		/*
		 * Normalización de los datos, de esta manera cada uno de los estados
		 * sus posibles transiciones deben sumar 1, de esta manera tendremos
		 * datos normalizados.
		 */
		for (int i = 0; i < 3; i++) {
			if (cont[i] != 0) {
				for (int j = 0; j < 3; j++) {
					transitionMatrix[i][j] /= (double) cont[i];
				}
			}
		}

		/*
		 * Un detalle importante, es que en caso que algún estado no se presente
		 * se debe realizar una variación en la inicialización de estados,
		 * debido que toda probabilidad daría cero. Por lo tanto, la
		 * distribución estacionaria daría cero si no puede trasicitar de un
		 * estado a otro.
		 * 
		 * Por ejemplo, que todas las transiciones del estado ocioso sean 0. De
		 * ser así, el estado inicial para calcular la distribución estacionaria
		 * será el estado estable.
		 */
		int i;
		if (rho[rho.length - 1] < lower) {
			i = 0;
		} else if ((rho[rho.length - 1] >= lower) && (rho[rho.length - 1] <= upper)) {
			i = 1;
		} else {
			i = 2;
		}

		/*
		 * Finalmente, se calculará la distribución estacionaria dada la
		 * ecuación de Chapman-Kolmogorov.
		 */
		double u;
		double probAcum;
		cont = new int[3];
		for (int k = 0; k < iteration; k++) {

			u = Math.random();
			probAcum = 0;
			for (int j = 0; j < 3; j++) {
				probAcum += transitionMatrix[i][j];
				if (u <= probAcum) {
					cont[j]++;
					i = j;
					break;
				}
			}

		}

		/*
		 * Normalización de los datos, para dejarlos con una probabilidad entre
		 * 0 y 1, que la suma de todos de 1
		 */

		for (int k = 0; k < 3; k++) {
			prediction[k] = (double) cont[k] / (double) iteration;
		}

		return prediction;
	}

	public double[][] getTransitionMatrix() {
		return transitionMatrix;
	}

	public void setTransitionMatrix(double transitionMatrix[][]) {
		this.transitionMatrix = transitionMatrix;
	}

	public double[] getPrediction() {
		return prediction;
	}

	public void setPrediction(double prediction[]) {
		this.prediction = prediction;
	}

	public String showPrediction() {
		DecimalFormat decimalFormat = new DecimalFormat("0.000");
		return "[ " + decimalFormat.format(prediction[0]) + " " + decimalFormat.format(prediction[1]) + " "
				+ decimalFormat.format(prediction[2]) + " ]";
	}

	@Override
	public String toString() {
		DecimalFormat decimalFormat = new DecimalFormat("0.000");

		String transitionMatrixString = new String(
				"╔═══════════════════╗\n║ " + decimalFormat.format(this.transitionMatrix[0][0]) + " "
						+ decimalFormat.format(this.transitionMatrix[0][1]) + " "
						+ decimalFormat.format(this.transitionMatrix[0][2]) + " ║\n║ "
						+ decimalFormat.format(this.transitionMatrix[1][0]) + " "
						+ decimalFormat.format(this.transitionMatrix[1][1]) + " "
						+ decimalFormat.format(this.transitionMatrix[1][2]) + " ║\n║ "
						+ decimalFormat.format(this.transitionMatrix[2][0]) + " "
						+ decimalFormat.format(this.transitionMatrix[2][1]) + " "
						+ decimalFormat.format(this.transitionMatrix[2][2]) + " ║\n╚═══════════════════╝");
		return transitionMatrixString;
	}
}
