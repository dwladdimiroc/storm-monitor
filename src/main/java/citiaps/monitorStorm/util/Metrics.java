package citiaps.monitorStorm.util;

import java.io.File;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.storm.shade.com.codahale.metrics.CsvReporter;
import org.apache.storm.shade.com.codahale.metrics.Gauge;
import org.apache.storm.shade.com.codahale.metrics.MetricRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import citiaps.monitorStorm.StormAdaptative;
import citiaps.monitorStorm.eda.Stats;
import citiaps.monitorStorm.eda.TopologyApp;

public class Metrics {
	private final static Logger logger = LoggerFactory.getLogger(Metrics.class);
	private final boolean showLogger = false;

	static final MetricRegistry metricsRegistry = new MetricRegistry();

	private Map<String, Long> input;
	private Map<String, Long> throughput;
	private Map<String, Double> utilization;
	private Map<String, Integer> replication;

	private CsvReporter reporter;

	public Metrics(String pathFolder) {
		new File(pathFolder).mkdir();

		this.input = new HashMap<String, Long>();
		this.throughput = new HashMap<String, Long>();
		this.utilization = new HashMap<String, Double>();
		this.replication = new HashMap<String, Integer>();

		this.reporter = CsvReporter.forRegistry(metricsRegistry).formatFor(Locale.US).convertRatesTo(TimeUnit.SECONDS)
				.convertDurationsTo(TimeUnit.SECONDS).build(new File(pathFolder));
	}

	public void start() {
		try {
			Thread.sleep(1000);
			this.reporter.start(5, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void createStatsBolt(final String nameBolt) {
		this.throughput.put(nameBolt, Long.valueOf(0));
		Metrics.metricsRegistry.register(MetricRegistry.name(StormAdaptative.class, nameBolt + "@throughput"),
				new Gauge<Long>() {
					@Override
					public Long getValue() {
						return throughput.get(nameBolt);
					}
				});

		this.utilization.put(nameBolt, Double.valueOf(0));
		Metrics.metricsRegistry.register(MetricRegistry.name(StormAdaptative.class, nameBolt + "@utilization"),
				new Gauge<Double>() {
					@Override
					public Double getValue() {
						return utilization.get(nameBolt);
					}
				});

		this.replication.put(nameBolt, Integer.valueOf(0));
		Metrics.metricsRegistry.register(MetricRegistry.name(StormAdaptative.class, nameBolt + "@replication"),
				new Gauge<Integer>() {
					@Override
					public Integer getValue() {
						return replication.get(nameBolt);
					}
				});
	}

	public void createStatsSpout(final String nameSpout) {
		this.input.put(nameSpout, Long.valueOf(0));
		Metrics.metricsRegistry.register(MetricRegistry.name(StormAdaptative.class, nameSpout + "@input"),
				new Gauge<Long>() {
					@Override
					public Long getValue() {
						return input.get(nameSpout);
					}
				});
	}

	public void sendStats(TopologyApp topologyApp) {
		for (String bolt : topologyApp.keySet()) {
			sendStatsBolt(bolt, topologyApp.getStats(bolt));
		}

		for (String spout : topologyApp.getInputStats().keyInput()) {
			sendStatsSpout(spout, topologyApp.getInputStats().getStreamInput(spout));
		}

		topologyApp.getInputStats().clear();
	}

	private void sendStatsSpout(String spout, long input) {
		if (showLogger)
			logger.info("[Spout={}],[Input={}]", spout, input);

		this.input.put(spout, input);
	}

	private void sendStatsBolt(String bolt, Stats statsBolt) {
		if (showLogger)
			logger.info("[Bolt={}],[Executed={}],[LastSimple={}]", bolt, statsBolt.getThroughput(),
					statsBolt.getLastSample());

		this.throughput.put(bolt, statsBolt.getThroughput());
		this.utilization.put(bolt, statsBolt.getLastSample());
		this.replication.put(bolt, statsBolt.getReplicas());
	}

}
