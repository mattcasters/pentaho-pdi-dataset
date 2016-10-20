# pentaho-pdi-dataset

Set of PDI plugins to more easily work with data sets. 
We also want to provide unit testing capabilities through input data sets and golden data sets.

# build instructions

ant : to build to jar file in dist/
ant clean : remove class files and built artifacts
ant clean-all : remove anything not part of this codebase

# Installation instructions

copy dist/pentaho-pdi-dataset-TRUNK-SNAPSHOT.jar to the plugins/ folder of your Pentaho Data Integration (Kettle) distribution.  Any location in that folder or subfolder is fine to make the plugin work.  Currently there are no extra libraries that need to be deployed.
