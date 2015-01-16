#!/bin/bash

M2=/Users/bpossolo/.m2/repository
GWT=$M2/com/google/gwt/gwt-user/2.7.0/gwt-user-2.7.0.jar
JDO=$M2/javax/jdo/jdo-api/3.0.1/jdo-api-3.0.1.jar
JPA=$M2/org/apache/geronimo/specs/geronimo-jpa_2.0_spec/1.0/geronimo-jpa_2.0_spec-1.0.jar
REMOTE_API=$M2/com/google/appengine/appengine-remote-api/1.9.17/appengine-remote-api-1.9.17.jar
GAE=$M2/com/google/appengine/appengine-api-1.0-sdk/1.9.17/appengine-api-1.0-sdk-1.9.17.jar
CLASSES=target/classes
SGM_CLASSES=/Users/bpossolo/projects/styleguise-marketplace/target/marketplace/WEB-INF/classes

java -cp $GWT:$JDO:$JPA:$REMOTE_API:$GAE:$CLASSES:$SGM_CLASSES net.styleguise.tools.DatastoreMigrator
