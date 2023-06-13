rm -rf result
mvn package
cp ./target/DE-KP-1.0.jar .
$SPARK_HOME/bin/spark-submit --class max.work.App DE-KP-1.0.jar 
rm -rf DE-KP-1.0.jar
mvn clean
