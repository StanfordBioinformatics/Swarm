#!/bin/sh
# Installs the Athena JDBC jar file to the local maven repo
mvn install:install-file -Dfile=./AthenaJDBC42_2.0.7.jar -DgroupId=com.simba -DartifactId=athena -Dversion=1.0 -Dpackaging=jar
