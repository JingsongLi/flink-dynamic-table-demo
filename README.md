Build Image For Flink Manged Table Demo

## Flink image
1. Build the flink jar
2. Copy jars in $FLINK/lib into ./flink-managed-table-demo/images/flink/lib/
3. Copy jars in $FLINK/opt into ./flink-managed-table-demo/images/flink/opt/
4. Copy connector/format and table-storage jars into ./flink-managed-table-demo/images/flink/connector_jar/
5. Build Flink image: `docker build -t flink:$FLINK_VERSION`

## Sql Client image
1. Modify `images/sql-client/Dockerfile`, substitute $FLINK_VERSION with the version built above
2. Build sql-client image: `docker build -t sql-client:$CLIENT_VERSION`

## DataGen image
1. Build the jar from source ./flink-managed-table-demo/datagen
2. Copy jar to ./flink-managed-table-demo/images/datagen/datagen.jar, and build image: `docker build -t datagen:$DATAGEN_VERSION`
