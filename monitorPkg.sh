mvn clean package
rm storm-monitor-1.0.jar
cp target/storm-monitor-1.0-jar-with-dependencies.jar ./
mv storm-monitor-1.0-jar-with-dependencies.jar storm-monitor-1.0.jar