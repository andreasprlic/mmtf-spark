# mmtf-spark
In this module we provide APIs and  examples of using Apache Spark, MMTF and Hadoop for high-performance structural bioinformatics.

# Examples of use
### First download and untar a Hadoop sequence file of the PDB (~7 GB download) 
```
wget http://mmtf.rcsb.org/v0.2/hadoopfiles/full.tar
tar -xvf full.tar
```
Or you can get a C-alpha, phosphate, ligand only version (~800 Mb download)
```
wget http://mmtf.rcsb.org/v0.2/hadoopfiles/reduced.tar
tar -xvf reduced.tar
```
### Second add the mmtf-spark dependecy to your pom

```xml
		<dependency>
			<groupId>org.rcsb</groupId>
			<artifactId>mmtf-spark</artifactId>
			<version>0.0.2</version>
		</dependency>
```


# Analysis
### First parse the data (changing the path to your file - either reduced or full).
```
		StructureDataRDD structureData = new StructureDataRDD("/path/to/hadoopfolder");
```

### You can split this into C-alpha protein chains of fixed langths
		SegmentDataRDD calphaChains = structureData.getCalpha().filterLength(10, 300);
		JavaDoubleRDD lengthDist = calphaChains.getLengthDist().cache();
		System.out.println("Mean chain length is:"+lengthDist.mean());
```

### Or you can fragment the protein into continuous overlapping fragments of length 8 
```
		SegmentDataRDD framgents = structureData.getFragments(8);
		SegmentClusters fragClusters =  framgents.groupBySequence();
		System.out.println("Number of different sequences of length 8: "+fragClusters.size());
```
