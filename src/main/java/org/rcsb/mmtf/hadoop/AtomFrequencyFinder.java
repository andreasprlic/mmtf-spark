package org.rcsb.mmtf.hadoop;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.biojava.nbio.structure.Atom;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.dataholders.MmtfStructure;
import org.rcsb.mmtf.decoder.DefaultDecoder;
import org.rcsb.mmtf.decoder.ReaderUtils;
import org.rcsb.mmtf.serialization.MessagePackSerialization;

import scala.Tuple2;

/**
 * A class to find the frequency of a given atom defined by
 * a {@link AtomSelectObject} across a whole Hadoop sequence file.
 * @author Anthony Bradley
 */
public class AtomFrequencyFinder {

	
	/**
	 * A function to read a hadoop sequence file to Biojava structures.
	 * @param args 1) is the path to the hadoop sequence file.
	 * @throws IOException
	 */
	public static void main(String[] args ) throws IOException
	{
		
		// Set the things we wan't to find
		AtomSelectObject selectObjectOne = new AtomSelectObject();
		// The input path for the data.
		String inPath = args[0];
		long startTime = System.currentTimeMillis();
		// This is the default 2 line structure for Spark applications
		SparkConf conf = new SparkConf().setMaster("local[*]")
				.setAppName(ContactFrequencyFinder.class.getSimpleName());
		// Set the config for the spark context
		JavaSparkContext sc = new JavaSparkContext(conf);
		Map<String, Object> atomFreqRdd = sc
				.sequenceFile(inPath, Text.class, BytesWritable.class, 8)
				// Use this for testing (set's the fraction of the data to process) 1.0 means sample everything
				.sample(false, 1.0)
				// Roughly thirty seconds
				.mapToPair(t -> new Tuple2<String, byte[]>(t._1.toString(), ReaderUtils.deflateGzip(t._2.getBytes())))
				// Roughly a minute 
				.mapToPair(t -> new Tuple2<String, MmtfStructure>(t._1, new MessagePackSerialization().deserialize(new ByteArrayInputStream(t._2))))
				// Roughly a minute
				.mapToPair(t -> new Tuple2<String, StructureDataInterface>(t._1,  new DefaultDecoder(t._2)))
				// Example function iterating through and finding the distances between the sites described above
				.flatMap(new CalculateFrequency(selectObjectOne))
				// Get the elements
//				.mapToPair(t -> new Tuple2<String,Atom>(t.getElement().toString(), t))
				.mapToPair(t -> new Tuple2<String,Atom>(t.getGroup().getPDBName(), t))
				.countByKey();
		// Now write out the contacts
		System.out.println(atomFreqRdd);
		System.out.println(atomFreqRdd.keySet().size());
		long endTime = System.currentTimeMillis();
		System.out.println("Proccess took "+(endTime-startTime)+" ms.");
		// Now close spark down
		sc.close();
	}
}