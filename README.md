[![Build Status](https://travis-ci.org/rcsb/mmtf-spark.svg?branch=master)](https://travis-ci.org/rcsb/mmtf-spark)
[![Version](http://img.shields.io/badge/version-0.0.4-blue.svg?style=flat)](https://github.com/rcsb/mmtf-spark/) [![License](http://img.shields.io/badge/license-Apache 2.0-blue.svg?style=flat)](https://github.com/rcsb/mmtf-spark/blob/master/LICENSE.txt)
[![Status](http://img.shields.io/badge/status-experimental-red.svg?style=flat)](https://github.com/rcsb/mmtf-spark)
# mmtf-spark
In this module we provide APIs and  examples of using Apache Spark, MMTF and Hadoop for high-performance structural bioinformatics.

# Examples of use
### First download and untar a Hadoop sequence file of the PDB (~7 GB download) 
```bash
wget http://mmtf.rcsb.org/v0.2/hadoopfiles/full.tar
tar -xvf full.tar
```
Or you can get a C-alpha, phosphate, ligand only version (~800 Mb download)
```bash
wget http://mmtf.rcsb.org/v0.2/hadoopfiles/reduced.tar
tar -xvf reduced.tar
```
### Second add the mmtf-spark dependecy to your pom

```xml
		<dependency>
			<groupId>org.rcsb</groupId>
			<artifactId>mmtf-spark</artifactId>
			<version>0.0.4</version>
		</dependency>
```


# Analysis
### First parse the data (changing the path to your file - either reduced or full).
```java
		StructureDataRDD structureData = new StructureDataRDD("/path/to/hadoopfolder");
```

### You can split this into C-alpha protein chains of fixed langths
```java
		SegmentDataRDD calphaChains = structureData.getCalpha().filterLength(10, 300);
		JavaDoubleRDD lengthDist = calphaChains.getLengthDist().cache();
		System.out.println("Mean chain length is:"+lengthDist.mean());
```

### Or you can fragment the protein into continuous overlapping fragments of length 8 
```java
		SegmentDataRDD framgents = structureData.getFragments(8);
		SegmentClusters fragClusters =  framgents.groupBySequence();
		System.out.println("Number of different sequences of length 8: "+fragClusters.size());
```
