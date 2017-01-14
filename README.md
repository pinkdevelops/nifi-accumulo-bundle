NiFi-Accumulo-Bundle
===================
Apache Accumulo processors for Apache NiFi


This processor connects to Apache Accumulo for putting/getting data.  Current build for Accumulo 1.7.0.

Currently only putting documents is working, todo:

 - UTF Encoding
 - Security/Kerberos
 - Scanner/iterator
 - tests

----------


Building this
-------------

 1. Build/compile the Accumulo Controller Service which is required for this: https://github.com/pinkdevelops/nifi-accumulo-service
 2. git clone https://github.com/pinkdevelops/nifi-accumulo-bundle
 2. cd nifi-accumulo-bundle
 3. mvn clean install -DskipTests
 4. cp nifi-accumulo-nar/target/nifi-accumulo-nar-1.0-SNAPSHOT.nar $NIFI_HOME/lib
