mkdir -p classes
rm -Rf classes/*
javac -classpath $($HADOOP_HOME/bin/hadoop classpath) -d classes ./src/**/*.java 
jar -cvf FpGrowth.jar -C classes .
