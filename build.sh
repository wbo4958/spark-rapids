set +ex
SCALA_VERSION=2.12
SPARK_VERSION=3.0.1-databricks
BASE_SPARK_POM_VERSION=3.0.1
echo "Spark version is $SPARK_VERSION"
echo "scala version is: $SCALA_VERSION"
cd /home/ubuntu/spark-rapids
sudo apt install -y maven
export WORKSPACE=`pwd`
mvn -B '-Pdatabricks301,!snapshot-shims' clean package -DskipTests || true
M2DIR=/home/ubuntu/.m2/repository
# pull normal Spark artifacts and ignore errors then install databricks jars, then build again
JARDIR=/databricks/jars
SQLJAR=----workspace_spark_3_0--sql--core--core-hive-2.3__hadoop-2.7_${SCALA_VERSION}_deploy.jar
CATALYSTJAR=----workspace_spark_3_0--sql--catalyst--catalyst-hive-2.3__hadoop-2.7_${SCALA_VERSION}_deploy.jar
ANNOTJAR=----workspace_spark_3_0--common--tags--tags-hive-2.3__hadoop-2.7_${SCALA_VERSION}_deploy.jar
COREJAR=----workspace_spark_3_0--core--core-hive-2.3__hadoop-2.7_${SCALA_VERSION}_deploy.jar
# install the 3.0.0 pom file so we get dependencies
COREPOM=spark-core_${SCALA_VERSION}-${BASE_SPARK_POM_VERSION}.pom
COREPOMPATH=$M2DIR/org/apache/spark/spark-core_${SCALA_VERSION}/${BASE_SPARK_POM_VERSION}
mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$COREJAR \
   -DgroupId=org.apache.spark \
   -DartifactId=spark-core_$SCALA_VERSION \
   -Dversion=$SPARK_VERSION \
   -Dpackaging=jar \
   -DpomFile=$COREPOMPATH/$COREPOM
mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$CATALYSTJAR \
   -DgroupId=org.apache.spark \
   -DartifactId=spark-catalyst_$SCALA_VERSION \
   -Dversion=$SPARK_VERSION \
   -Dpackaging=jar
mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$SQLJAR \
   -DgroupId=org.apache.spark \
   -DartifactId=spark-sql_$SCALA_VERSION \
   -Dversion=$SPARK_VERSION \
   -Dpackaging=jar
mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$ANNOTJAR \
   -DgroupId=org.apache.spark \
   -DartifactId=spark-annotation_$SCALA_VERSION \
   -Dversion=$SPARK_VERSION \
   -Dpackaging=jar
mvn -B '-Pdatabricks301,!snapshot-shims' clean package -DskipTests
