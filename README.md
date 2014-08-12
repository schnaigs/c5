c5
================

C5 is a simple, scalable open source distributed database that is compatible with the HBase API. 
Like HBase, C5 is a BigTable-flavoured ACID database. 
C5 supports three types of compatibility: HBase API compatibility(HTable/HBaseAdmin), HFile compatibility, and HBase replication.

#### HBase API compatibility (HTable/HBaseAdmin)
Your code that talks to HTable and/or HBaseAdmin can talk to c5db-client!

#### HFile compatibility
C5 uses the same HFiles that HBase does!

#### HBase replication
The module hbase-replication-server runs a special server on a special port which acts like an HBase regionserver and supports
the HBase replication API!

Building and running
--------------------
To build this project, simply mvn install (with optional -DskipTests) to build the c5 daemons, client libraries and
optionally run all of our tests.

To start the c5server in single node mode, change into the 
c5db directory and run ./bin/run_test_server.sh, after building the project.
One can think of run_test_server as an example of how to start the server.

#### -D options of c5db include
regionServerPort=<port#>
webServerPort=<port#>
clusterName=<The name of the cluster>
-Dorg.slf4j.simpleLogger.defaultLogLevel=<log level>
-Dorg.slf4j.simpleLogger.log.org.eclipse.jetty=<log level>

Modules
-------
__c5db__ is the database server itself, and __c5db-client__ is the client.
The server makes use of replication, logging, and module structure; this code resides in
__c5db-replicator__, __c5-olog__, and __c5-modules__ respectively. Next we have __c5-hbase-shim__,
__c5db-cat-olog__, and __c5-general-replication__. A shim is "a washer or thin strip of material
used to align parts, make them fit, or reduce wear."

#### c5-proto

#### c5-protostuff-codec

#### c5-util

#### c5-general-replication

#### c5db-cat-olog

#### c5-hbase-shim

#### c5db

#### modules that start with hbase-

Examples
--------
Examples of how to access the server can be found in c5-client-tests. In 
addition, a web status console will start on port 31337. Information about
the cluster can be found there.

Troubleshooting
---------------
On Mac OSX:
export JAVA_HOME=`/usr/libexec/java_home -v 1.8`

More documentation
------------------
For more information about c5's replication and logging, see the package-info.java files in the c5db-replication module
and the c5db-olog module respectively.

We're on Github at https://github.com/OhmData/c5.

Licensing
---------
This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public
License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program.  If not, see http://www.gnu.org/licenses/.
