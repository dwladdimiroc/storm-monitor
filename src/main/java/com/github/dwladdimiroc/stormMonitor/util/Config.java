package com.github.dwladdimiroc.stormMonitor.util;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;

public class Config {
	private int windowMonitor;

	private double lower;
	private double upper;

	private int samplesReactive;
	private int alertReactive;
	private int replicaReactive;

	private int samplesPredictive;
	private int iterationPredictive;
	private double thresholdPredictive;
	private int replicaPredictive;

	private double Esync;

	public Config() {
		this.windowMonitor = 0;

		this.lower = 0;
		this.upper = 0;

		this.samplesReactive = 0;
		this.alertReactive = 0;
		this.replicaReactive = 0;

		this.samplesPredictive = 0;
		this.iterationPredictive = 0;
		this.thresholdPredictive = 0;
		this.replicaPredictive = 0;

		this.Esync = 0;
	}

	public void readConfig(String path) {
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db;
		try {
			db = dbf.newDocumentBuilder();
			Document doc = db.parse(path);

			this.setWindowMonitor(Integer.parseInt(getValue(doc, "windowMonitor")));

			this.setLower(Double.parseDouble(getValue(doc, "lower")));
			this.setUpper(Double.parseDouble(getValue(doc, "upper")));

			this.setSamplesReactive(Integer.parseInt(getValue(doc, "samplesReactive")));
			this.setAlertReactive(Integer.parseInt(getValue(doc, "alertReactive")));
			this.setReplicaReactive(Integer.parseInt(getValue(doc, "replicaReactive")));

			this.setSamplesPredictive(Integer.parseInt(getValue(doc, "samplesPredictive")));
			this.setIterationPredictive(Integer.parseInt(getValue(doc, "iterationPredictive")));
			this.setThresholdPredictive(Double.parseDouble(getValue(doc, "thresholdPredictive")));
			this.setReplicaPredictive(Integer.parseInt(getValue(doc, "replicaPredictive")));

			this.setEsync(Double.parseDouble(getValue(doc, "Esync")));

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private String getValue(Document doc, String key) {
		return doc.getDocumentElement().getElementsByTagName(key).item(0).getTextContent();
	}

	public int getWindowMonitor() {
		return windowMonitor;
	}

	public void setWindowMonitor(int windowMonitor) {
		this.windowMonitor = windowMonitor;
	}

	public double getLower() {
		return lower;
	}

	public void setLower(double lower) {
		this.lower = lower;
	}

	public double getUpper() {
		return upper;
	}

	public void setUpper(double upper) {
		this.upper = upper;
	}

	public int getSamplesReactive() {
		return samplesReactive;
	}

	public void setSamplesReactive(int samplesReactive) {
		this.samplesReactive = samplesReactive;
	}

	public int getAlertReactive() {
		return alertReactive;
	}

	public void setAlertReactive(int alertReactive) {
		this.alertReactive = alertReactive;
	}

	public int getReplicaReactive() {
		return replicaReactive;
	}

	public void setReplicaReactive(int replicaReactive) {
		this.replicaReactive = replicaReactive;
	}

	public int getSamplesPredictive() {
		return samplesPredictive;
	}

	public void setSamplesPredictive(int samplesPredictive) {
		this.samplesPredictive = samplesPredictive;
	}

	public int getIterationPredictive() {
		return iterationPredictive;
	}

	public void setIterationPredictive(int iterationPredictive) {
		this.iterationPredictive = iterationPredictive;
	}

	public double getThresholdPredictive() {
		return thresholdPredictive;
	}

	public void setThresholdPredictive(double thresholdPredictive) {
		this.thresholdPredictive = thresholdPredictive;
	}

	public int getReplicaPredictive() {
		return replicaPredictive;
	}

	public void setReplicaPredictive(int replicaPredictive) {
		this.replicaPredictive = replicaPredictive;
	}

	public double getEsync() {
		return Esync;
	}

	public void setEsync(double esync) {
		Esync = esync;
	}

	@Override
	public String toString() {
		return "[{windowMonitor=" + windowMonitor + "}, {lower=" + lower + "}, {upper=" + upper + "}, {samplesReactive="
				+ samplesReactive + "}, {alertReactive=" + alertReactive + "}, {replicaReactive" + replicaReactive
				+ "}, {samplesPredictive=" + samplesPredictive + "}, {iterationPredictive=" + iterationPredictive
				+ "}, {thresholdPredictive=" + thresholdPredictive + "}, {replicaPredictive=" + replicaPredictive
				+ "}]";
	}

}
