package org.rcsb.mmtf.hadoop;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.biojava.nbio.structure.contact.AtomContact;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.dataholders.MmtfStructure;
import org.rcsb.mmtf.decoder.DefaultDecoder;
import org.rcsb.mmtf.decoder.ReaderUtils;
import org.rcsb.mmtf.serialization.MessagePackSerialization;

import scala.Tuple2;


/**
 * Class to find and count inter-atom contacts.
 * @author Anthony Bradley
 *
 */
public class ContactFrequencyFinder  implements Serializable {    

	/**
	 * Serial ID for this version of the class.
	 */
	private static final long serialVersionUID = 3037567648753603114L;

	/**
	 * A function to read a hadoop sequence file to Biojava structures.
	 * @param args 1) is the path to the hadoop sequence file.
	 * @throws IOException
	 */
	public static void main(String[] args ) throws IOException
	{

		// Set the things we wan't to find 
		AtomSelectObject selectObjectOne = new AtomSelectObject();
		// Get all the polymer groups
		selectObjectOne.setGroupType("polymer");
		AtomSelectObject selectObjectTwo = new AtomSelectObject();
		// Get all the non-polymer i.e. ligand groups
		selectObjectTwo.setGroupType("non-polymer");
		// Set the cutoff
		double cutoff = 5.0;
		// The input path for the data.
		String inPath = args[0];
		long startTime = System.currentTimeMillis();
		// This is the default 2 line structure for Spark applications
		SparkConf conf = new SparkConf().setMaster("local[*]")
				.setAppName(ContactFrequencyFinder.class.getSimpleName());
		// Set the config for the spark context
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaPairRDD<String, Double> atomConactRdd = sc
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
				.flatMap(new CalculateContacts(selectObjectOne, selectObjectTwo, cutoff))
				// Get a string describing the interaction and the distance of the interaction
				.mapToPair(t -> new Tuple2<String,Double>(getInteractionType(t), t.getDistance()))
				.cache();
		// Now we can a series of reduce functions to find summaries of all the contacts in the PDB
		PrintWriter out = new PrintWriter("summary.txt");
		// Reduce the contacts down to counts
		out.println(atomConactRdd.countByKey());
		out.close();
		// Write the distributions down to a file
		JavaPairRDD<String, Iterable<Double>> groupByKey = atomConactRdd.groupByKey();
		groupByKey.saveAsTextFile("contact_dists.txt");
		long endTime = System.currentTimeMillis();
		System.out.println("Proccess took "+(endTime-startTime)+" ms.");
		// Now close spark down
		sc.close();
	}

	/**
	 * Return a canonicalised description of inter group contacts.
	 * @param atomContact the {@link AtomContact} object
	 * @return the canonicalised string describing the contacts 
	 */
	private static String getInteractionType(AtomContact atomContact) {
		List<String> intList = new ArrayList<>();
		intList.add(atomContact.getPair().getFirst().getName()+"_"+atomContact.getPair().getFirst().getGroup().getPDBName());
		intList.add(atomContact.getPair().getSecond().getName()+"_"+atomContact.getPair().getSecond().getGroup().getPDBName());
		Collections.sort(intList);
		return String.join("__", intList);
	}
}
