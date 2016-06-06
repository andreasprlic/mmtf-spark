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
### You can split either file into C-alpha protein chains
```
		StructureDataRDD structureDataRDD = new StructureDataRDD("/path/to/hadoopfolder");
		// Filter lenghts of between 10 and 300
		SegmentDataRDD calphaChains = structureDataRDD.getCalpha().filterLength(10, 300);
		// And analyse their lengths
		JavaDoubleRDD lengthDist = calphaChains.getLengthDist().cache();
		System.out.println("Mean chain length is:"+lengthDist.mean());
```

### Or you can fragment the protein into continuous overlapping fragments of length 8 
```
		StructureDataRDD structureDataRDD = new StructureDataRDD("/path/to/hadoopfolder");
		SegmentDataRDD framgents = structureDataRDD.getFragments(8);
		SegmentClusters fragClusters =  framgents.groupBySequence();
		System.out.println("Number of different sequences of length 8: "+fragClusters.size());
```
