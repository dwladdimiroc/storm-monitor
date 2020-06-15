# Self Adaptive Storm

In this folder is the self-adaptation of SPS Storm, which MALE model modify the system about the overhead that have each operators, where this rebalance the topology according to available resources.

For the configuration of MAPE model exist the file `conf/mape.xml`. Below is an example of this with its default values:

	<?xml version="1.0"?>
	<config>
		<windowMonitor>5</windowMonitor>
		<upper>0.7</upper>
		<lower>0.3</lower>
		<samplesReactive>5</samplesReactive>
		<samplesPredictive>120</samplesPredictive>
		<replicaReactive>1</replicaReactive>
		<replicaPredictive>5</replicaPredictive>
		<alertReactive>2</alertReactive>
		<thresholdPredictive>0.25</thresholdPredictive>
		<iterationPredictive>100000</iterationPredictive>
		<Esync>5.0</Esync>
	</config>

To run monitor execute the command:

	java -jar storm-monitor-1.1.1.jar idAppStorm ipNimbus portNimbus stats

where `idAppStorm` is Storm's id and stats is optional.