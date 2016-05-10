package org.rcsb.mmtf.spark.data;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;

import scala.Tuple2;



/**
 * A class to hold information and utilities around {@link Segment} 
 * information.
 * @author Anthony Bradley
 *
 */
public class SegmentDataRDD {

	/** The underlying {@link JavaPairRDD} data. */
 	private JavaPairRDD<String, Segment> segmentRDD;
	
	/**
	 * A constructor for the class.
	 * @param segmentRDD the input underlying {@link JavaPairRDD}
	 */
	public SegmentDataRDD(JavaPairRDD<String, Segment> segmentRDD) {
		this.segmentRDD = segmentRDD;
	}

	/**
	 * Get the {@link JavaPairRDD} of the {@link String} {@link Segment} data.
	 * For lower level processing
	 * @return the segmentRDD the underlying {@link JavaPairRDD} of {@link String}  {@link Segment}
	 * that can be processed on.
	 */
	public JavaPairRDD<String, Segment> getSegmentRDD() {
		return segmentRDD;
	}
	
	/**
	 * Get the length distribution of this RDD.
	 * @return the {@link JavaDoubleRDD} of the lengths
	 */
	public JavaDoubleRDD getLengthDist() {
		return segmentRDD.mapToDouble(t -> t._2.getStructure().length);
	}
	
	/**
	 * Filter the RDD based on a minimum length.
	 * @param min the minimum length to allow.
	 * @return the {@link SegmentDataRDD} after filtering
	 */
	public SegmentDataRDD filterMinLength(int min) {
		return new SegmentDataRDD(segmentRDD.filter(t -> t._2.getStructure().length>min));
	}
	
	/**
	 * Filter the RDD based on a maximum length.
	 * @param max the maximum length to allow
	 * @return the {@link SegmentDataRDD} after filtering
	 */
	public SegmentDataRDD filterMaxLength(int max) {
		return new SegmentDataRDD(segmentRDD.filter(t -> t._2.getStructure().length<max));
		
	}
	
	/**
	 * Filter the RDD based on a minimum and maximum length.
	 * @param min the minimum length to allow
	 * @param max the maximum length to allow
	 * @return the {@link SegmentDataRDD} after filtering
	 */
	public SegmentDataRDD filterLength(int min, int max) {
		return new SegmentDataRDD(segmentRDD.filter(t -> t._2.getStructure().length<max 
				&& t._2.getStructure().length>min));
	}
	

	/**
	 * Filter the segments so a non-redundant set is available.
	 * @param the similarity (e.g. sequence identity) to permit
	 * @return the {@link SegmentDataRDD} of non-redundant sequences
	 */
	public SegmentDataRDD findRedundantSet(double similarity) {
		System.err.println("Currently not functioning");
		return new SegmentDataRDD(segmentRDD);
	}
	
	/**
	 * Group by sequence identity.
	 * @param the similarity (e.g. sequence identity) to permit
	 * @return the {@link SegmentDataRDD} of non-redundant sequences
	 */
	public SegmentClusters groupBySequence() {
		JavaPairRDD<String, Segment> seqMap = segmentRDD.mapToPair(t -> {
			String sequence = t._2.getSequence();
			Segment segment = t._2;
			return new Tuple2<String,Segment>(sequence, segment);
		});
		return new SegmentClusters(seqMap.groupByKey());
	}
	
}
