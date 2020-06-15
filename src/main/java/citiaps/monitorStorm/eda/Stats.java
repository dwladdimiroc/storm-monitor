package citiaps.monitorStorm.eda;

import java.util.Queue;

import org.apache.commons.collections4.queue.CircularFifoQueue;

public class Stats {
	private String id;
	private int replicas;

	private long executed;
	private double timeAvg;

	private long throughput;
	private long executedTotal;

	private Queue<Double> history;
	private Queue<Integer> markMap;

	public Stats() {
		this.setId("bolt-default");
		this.setReplicas(0);

		this.setExecuted(0);
		this.setTimeAvg(0);

		this.setThroughput(0);
		this.setExecutedTotal(0);

		this.setHistory(new CircularFifoQueue<Double>(120));
		this.setMarkMap(new CircularFifoQueue<Integer>(2));
	}

	public Stats(int samplesPredictive, int alertReactive) {
		this.setId("bolt-default");
		this.setReplicas(0);

		this.setHistory(new CircularFifoQueue<Double>(samplesPredictive));
		this.setMarkMap(new CircularFifoQueue<Integer>(alertReactive));

		this.setExecutedTotal(0);
	}

	public void addSample(double capacity) {
		if (capacity > 1)
			capacity = 1;
		this.getHistory().add(capacity);
	}

	public double getLoad(int samplesReactive) {
		int size = getHistory().size();
		Double[] lastSamples = new Double[size];
		lastSamples = getHistory().toArray(lastSamples);

		double Cavg = 0;
		for (int i = size - 1; i > ((size - 1) - samplesReactive); i--) {
			Cavg += lastSamples[i];
		}
		Cavg /= samplesReactive;

		return Cavg;
	}

	public void clearTemp() {
		this.setExecuted(0);
		this.setTimeAvg(0);
	}

	public void clearStats() {
		this.setThroughput(0);
		this.setExecutedTotal(0);
	}

	public void clear() {
		this.setReplicas(1);

		this.history.clear();
		this.setHistory(null);

		this.markMap.clear();
		this.setMarkMap(null);

		this.setExecutedTotal(0);
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public int getReplicas() {
		return replicas;
	}

	public void setReplicas(int replicas) {
		this.replicas = replicas;
	}

	public long getExecuted() {
		return executed;
	}

	public void setExecuted(long executed) {
		this.executed = executed;
	}

	public double getTimeAvg() {
		return timeAvg;
	}

	public void setTimeAvg(double timeAvg) {
		this.timeAvg = timeAvg;
	}

	/**
	 * @return the throughput
	 */
	public long getThroughput() {
		return throughput;
	}

	/**
	 * @param throughput
	 *            the throughput to set
	 */
	public void setThroughput(long throughput) {
		this.throughput = throughput;
	}

	public long getExecutedTotal() {
		return executedTotal;
	}

	public void setExecutedTotal(long executedTotal) {
		this.executedTotal = executedTotal;
	}

	public Queue<Double> getHistory() {
		return history;
	}

	public double getLastSample() {
		Double[] lastSamples = new Double[getHistory().size()];
		lastSamples = getHistory().toArray(lastSamples);
		return lastSamples[getHistory().size() - 1];
	}

	public void setHistory(Queue<Double> history) {
		this.history = history;
	}

	public Queue<Integer> getMarkMap() {
		return markMap;
	}

	public void setMarkMap(Queue<Integer> markMap) {
		this.markMap = markMap;
	}

	@Override
	public String toString() {
		return "[{Bolt=" + this.getId() + "}, {History=" + this.getHistory() + "}, {Replicas=" + this.getReplicas()
				+ "}]";
	}
}
