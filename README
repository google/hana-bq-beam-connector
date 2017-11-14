This is not an official Google product.

This project was meant to show the ability to move data from SAP Hana to Google
BigQuery using Apache Beam. It is a proof of concept and needs more work to be
able to handle all use cases and scenarios. 

The pom.xml file references the SAP Hana jdbc driver which you need to obtain
from the SAP client install and can be installed locally to match the pom.xml
entry with the following command:

mvn install:install-file -Dfile=ngdbc.jar \
	-DgroupId=sap \
	-DartifactId=sap-hana-jdbc \
	-Dversion=1.0 \
	-Dpackaging=jar \
	-DgeneratePom=true

Once the jdbc driver is installed, you can build and run the pipeline using the following maven command:

mvn compile exec:java \
	-Dexec.mainClass=third_party.connectors.HanaToBQ \
	-Dexec.args="--tempLocation=<i.e. gs://my-bucket/temp>
		--runner=DataflowRunner
		--project=<my-project-id>
		--stagingLocation=<i.e. gs://my-bucket/staging>
		--connectionString=jdbc:sap://x.x.x.x:30015/?databaseName=x
		--tableName=<hana table name>
		--username=<hana username>
		--password=<hana password>
		--destDataset=<bq dest dataset>
		--timestampColumn=<numeric timestamp column>
		--startTime=<epoch start time>
		--endTime=<epoch end time>
		--chunkSize=<approximate # of rows in query chunk>" \
	-Pdataflow-runner
