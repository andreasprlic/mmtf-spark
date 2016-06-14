package org.rcsb.mmtf.spark.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import javax.vecmath.Point3d;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SQLContext;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.dataholders.MmtfStructure;
import org.rcsb.mmtf.decoder.GenericDecoder;
import org.rcsb.mmtf.decoder.ReaderUtils;
import org.rcsb.mmtf.serialization.MessagePackSerialization;
import org.rcsb.mmtf.spark.mappers.FlatMapIntList;
import org.rcsb.mmtf.spark.mappers.MapToPairs;
import org.rcsb.mmtf.spark.data.AtomSelectObject;
import org.rcsb.mmtf.spark.data.SegmentDataRDD;
import org.rcsb.mmtf.spark.data.StructureDataRDD;

import org.rcsb.mmtf.utils.CodecUtils;

import scala.Tuple2;
import scala.reflect.ClassTag;

/**
 * A class of Spark utility methods
 * @author Anthony Bradley
 *
 */
public class SparkUtils {

	/** The file path of the Hadoop sequence file to read */
	private static String hadoopFilePath = null;
	private static SparkConf conf = null;
	private static JavaSparkContext javaSparkContext = null;
	private static SQLContext sqlContext = null;
	/** Where to get the data from. */
	public static final String URL_LOCATION = "http://mmtf.rcsb.org/v0/hadoopfiles/full.tar";
	private static final String hadoopBase = "/hadoop/v0";
	private static final String pdbFileName = "full";
	private static final String tarFileName = "full.tar";



	/**
	 * Convert a {@link JavaRDD} to a {@link Dataset} of the same type.
	 * @param javaRDD the input {@link JavaRDD}
	 * @param clazz the input class of the RDD
	 * @param <T> the type of the RDD
	 * @return the converted dataset
	 */
	public static <T> Dataset<T> convertToDataset(JavaRDD<T> javaRDD, Class<T> clazz) {
		SQLContext sqlContext = getSqlContext();
		return sqlContext.createDataset(JavaRDD.toRDD(javaRDD), Encoders.bean(clazz));
	}



	/**
	 * Get an {@link JavaPairRDD} of {@link String} {@link StructureDataInterface} from a file path.
	 * @param filePath the input path to the hadoop sequence file
	 * @return the {@link JavaPairRDD} of {@link String} {@link StructureDataInterface}
	 */
	public static JavaPairRDD<String, StructureDataInterface> getStructureDataRdd(String filePath) {
		return getMmtfStructureRdd(filePath)
				// Roughly a minute
				.mapToPair(t -> new Tuple2<String, StructureDataInterface>(t._1,  new GenericDecoder(t._2)));
	}

	/**
	 * Get a {@link JavaPairRDD} of {@link String} {@link MmtfStructure} from a file path
	 * @param filePath the input path to the hadoop sequence file
	 * @return the {@link JavaPairRDD} of {@link String} {@link MmtfStructure}
	 */
	public static JavaPairRDD<String, MmtfStructure> getMmtfStructureRdd(String filePath) {
		return getSparkContext()
				.sequenceFile(filePath, Text.class, BytesWritable.class, 8)
				// Roughly thirty seconds
				.mapToPair(t -> new Tuple2<String, byte[]>(t._1.toString(), ReaderUtils.deflateGzip(t._2.getBytes())))
				// Roughly a minute 
				.mapToPair(t -> new Tuple2<String, MmtfStructure>(t._1, new MessagePackSerialization().deserialize(new ByteArrayInputStream(t._2))));
	}



	/**
	 * Get the {@link StructureDataRDD} from a file path.
	 * @param filePath the input file path
	 * @return the {@link StructureDataRDD} object
	 */
	public static StructureDataRDD getStructureObj(String filePath) {
		return new StructureDataRDD(getStructureDataRdd(filePath));
	}

	/**
	 * Get the {@link SparkConf} for this run.
	 * @return the {@link SparkConf} for this run
	 */
	public static SparkConf getConf() {
		if (conf==null){
			// This is the default 2 line structure for Spark applications
			conf = new SparkConf().setMaster("local[*]")
					.setAppName(SparkUtils.class.getSimpleName()); 
		}
		return conf;

	}

	/**
	 * Get the {@link JavaSparkContext} for this run.
	 * @return the {@link JavaSparkContext} for this run
	 */
	public static JavaSparkContext getSparkContext(){
		if(javaSparkContext==null){
			javaSparkContext = new JavaSparkContext(SparkUtils.getConf());
		}
		return javaSparkContext;
	}

	/**
	 * Get the {@link SQLContext} for producing datasets and data frames.
	 * @return the {@link SQLContext}.
	 */
	public static SQLContext getSqlContext() {
		if(sqlContext==null){
			sqlContext = new SQLContext(getSparkContext());
		}
		return sqlContext;
	}


	/**
	 * Get the {@link JavaSparkContext} for this run.
	 * @param conf the {@link SparkConf} to use to setup the context
	 * @return the {@link JavaSparkContext} for this run
	 */
	public static JavaSparkContext getSparkContext(SparkConf conf){
		if(javaSparkContext==null){
			javaSparkContext = new JavaSparkContext(conf);
		}
		return javaSparkContext;
	}

	/**
	 * Gently shutdown at the end of a run.
	 */
	public static void shutdown() {
		javaSparkContext.close();
	}

	/**
	 * Set the file path of the Hadoop file to read.
	 * @param filePath the input file path to read
	 */
	public static void filePath(String filePath) {
		hadoopFilePath = filePath;
	}

	/**
	 * Get the calpha as a {@link Point3d}.
	 * @param structureDataInterface the {@link StructureDataInterface} to read
	 * @param groupType the integer specifying the grouptype
	 * @param atomCounter the atom count at the start of this group
	 * @return the point3d object specifying the calpha of this point
	 */
	public static Point3d getCalpha(StructureDataInterface structureDataInterface, int groupType, int atomCounter) {
		for(int i=0; i<structureDataInterface.getNumAtomsInGroup(groupType);i++){
			if(structureDataInterface.getGroupAtomNames(groupType)[i].equals("CA") && structureDataInterface.getGroupElementNames(groupType)[i].equals("C")){
				Point3d point3d = new Point3d();
				point3d.x = structureDataInterface.getxCoords()[atomCounter+i];
				point3d.y = structureDataInterface.getyCoords()[atomCounter+i]; 
				point3d.z = structureDataInterface.getzCoords()[atomCounter+i];
				return point3d;
			}
		}
		return null;

	}


	/**
	 * Compress a byte array using Gzip.
	 * @param byteArray the input byte array
	 * @return the compressed byte array
	 * @throws IOException an error reading from the URL
	 */
	public static byte[] gzipCompress(byte[] byteArray) throws IOException {
		// Function to gzip compress the data for the hashmaps
		ByteArrayOutputStream byteStream =
				new ByteArrayOutputStream(byteArray.length);
		try
		{
			GZIPOutputStream zipStream =
					new GZIPOutputStream(byteStream);
			try
			{
				zipStream.write(byteArray);
			}
			finally
			{
				zipStream.close();
			}
		}
		finally
		{
			byteStream.close();
		}
		byte[] compressedData = byteStream.toByteArray();
		return compressedData;
	}

	/**
	 * Get the path to the Hadoop sequence file to read.
	 * @return the {@link String} path of the Hadoop sequence file to read.
	 */
	public static String getFilePath() {
		return hadoopFilePath;
	}


	/**
	 * Function to download the PDB and place it on the file system.
	 * @throws IOException  an error reading from the URL
	 * @throws FileNotFoundException an error transferring files
	 */
	public static void downloadPdb() throws FileNotFoundException, IOException {
		// Get the base path
		File dstFile = new File(getHadoopBase());
		if (dstFile.exists()){
			dstFile.delete();
		}
		try {
			URL url = new URL(URL_LOCATION);
			System.out.println("Downloading PDB data....");
			FileUtils.copyURLToFile(url, dstFile);
		} catch (Exception e) {
			System.err.println(e);
		}
		// And untar it.
		untar(getFullPdbFile(), new TarArchiveInputStream(new FileInputStream(dstFile)));
	}

	/**
	 * Get the path for where the full PDB file will be stored.
	 * @return the {@link String} describing where the full PDB data is
	 */
	public static String getFullPdbFile() {
		URL baseURI = SparkUtils.class.getClassLoader().getResource(hadoopBase+File.separator+pdbFileName);
		if (baseURI==null){
			return null;
		}
		else{
			return baseURI.getPath();
		}
	}

	/**
	 * Get the base path of where to store Hadoop data.
	 * @return the {@link String} of the path of where Hadoop data should be
	 */
	public static String getHadoopBase() {
		URL hadoopUrl = SparkUtils.class.getClassLoader().getResource(hadoopBase);
		System.out.println(hadoopUrl);
		return hadoopUrl.getPath()+File.separator+tarFileName;
	}

	/**
	 * Untar a folder to the path.
	 * @param destinationFolder the folder to write to
	 * @param tarInputStream the {@link TarArchiveInputStream} input
	 * @throws IOException an error moving the file
	 */
	private static void untar(String destinationFolder, TarArchiveInputStream tarInputStream) throws IOException {
		System.out.println("Untarring PDB...");
		TarArchiveEntry tarEntry = tarInputStream.getNextTarEntry();
		while (tarEntry != null) {
			// create a file with the same name as the tarEntry
			File destPath = new File(destinationFolder + System.getProperty("file.separator") + tarEntry.getName());
			System.out.println("Extracting: " + destPath.getCanonicalPath());
			if (tarEntry.isDirectory()) {
				destPath.mkdirs();
			} else {
				destPath.createNewFile();
				FileOutputStream fout = new FileOutputStream(destPath);
				tarInputStream.read(new byte[(int) tarEntry.getSize()]);
				fout.close();
			}
			tarEntry = tarInputStream.getNextTarEntry();
		}
		tarInputStream.close();
	}


	/**
	 * Get the Calpha chains for a few structures as a SegmentDataRDD.
	 * @param inputIds the list of input ids as strings
	 * @throws IOException due to reading from the MMTF url
	 * @return the {@link SegmentDataRDD} of the calpha chains given the ids.
	 */
	public static SegmentDataRDD getCalphaChains(String[] inputIds) throws IOException {
		return new StructureDataRDD(inputIds).getCalpha();	
	}


	/**
	 * Helper function to get the data for a PDB id as an gzip compressed byte array.
	 * Data is retrieved from the REST service. This should be moved to mmtf for the next release.
	 * @param pdbCode the input PDB id
	 * @return the gzip compressed byte array for this structure
	 * @throws IOException  due to retrieving data from the URL
	 */
	public static byte[] getDataAsByteArray(String pdbCode) throws IOException {

		// Get these as an inputstream
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		InputStream is = null;
		URL url = new URL(CodecUtils.BASE_URL + pdbCode);
		try {
			is = url.openStream();
			byte[] byteChunk = new byte[2048]; // Or whatever size you want to read in at a time.
			int n;
			while ( (n = is.read(byteChunk)) > 0 ) {
				baos.write(byteChunk, 0, n);
			}
		} finally {
			if (is != null) { is.close(); }
		}
		return baos.toByteArray();
	}

	/**
	 * Utility function to generate an {@link AtomSelectObject}. 
	 * Mainly for application to the Python API.
	 * @param atomNameList the list of atoms to consider
	 * @param groupNameList the list of groups to consider (e.g. LYS)
	 * @param charged whether to consider charged atoms only (true)
	 * @param elementNameList the list of elements to consider
	 * @param groupType a string defining the type of group
	 * @return an atom select object of the appropriate type.
	 */
	public AtomSelectObject generateAtomSelectObject(List<String> atomNameList, 
			List<String> groupNameList, boolean charged, List<String> elementNameList,
			String groupType) {
		return new AtomSelectObject()
				.atomNameList(atomNameList)
				.charged(charged)
				.groupNameList(groupNameList)
				.groupType(groupType)
				.elementNameList(elementNameList);
	}

	/**
	 * Get a {@link JavaPairRDD} of Integers to do a half matrix comparison. i.e. all comparisons
	 * where i!=j and i is greather than j
	 * @param numMembers the total number of members to compare
	 * @return the {@link JavaPairRDD} of the comparisons
	 */
	public static JavaPairRDD<Integer, Integer> getComparisonMatrix(int numMembers) {
		JavaRDD<Integer> singleInt = getSparkContext().parallelize(Arrays.asList(numMembers));
		JavaRDD<Integer> multipleInts = singleInt.flatMap(new FlatMapIntList());
		JavaPairRDD<Integer, Integer> comparisons = multipleInts.flatMapToPair(new MapToPairs(numMembers));
		return comparisons;
	}


	/**
	 * Join a Spark output dir of text files into a single text file.
	 * @param dir the file the partitions are written to
	 * @param header the  header for the top of the file
	 * @throws IOException an error joining the files
	 */
	public static void combineDirToFile(File dir, String header) throws IOException{
		if(!dir.isDirectory()){
			System.err.println(dir.getAbsolutePath()+" is not dir");
			return;
		}
		File[] outList = Arrays.asList(dir.listFiles()).stream()
				.filter(file -> Paths.get(file.getAbsolutePath()).getFileName().toString().startsWith("part"))
				.toArray(size -> new File[size]);
		File outFile = new File(dir.getAbsolutePath()+".txt");
		outFile.delete();
		joinFiles(outFile, outList, header);
	}

	/**
	 * Join a list of files into a destination file
	 * @param destination the destination path
	 * @param sources the list of files to join together
	 * @param header the header for the file
	 * @throws IOException due to writing files to disk
	 */
	private static void joinFiles(File destination, File[] sources, String header) throws IOException {
		OutputStream output = null;

		try {
			output = new BufferedOutputStream(new FileOutputStream(destination, true));
			InputStream headerStream = null;
			try{
				headerStream = new ByteArrayInputStream(header.getBytes(StandardCharsets.UTF_8));
				IOUtils.copy(headerStream, output);
			}
			finally{
				IOUtils.closeQuietly(headerStream);
			}
			for (File source : sources) {
				appendFile(output, source);
			}
		} finally {
			IOUtils.closeQuietly(output);
		}
	}

	/**
	 * Append a file to an output stream.
	 * @param output the ouput stream
	 * @param source the file to append
	 * @throws IOException due to an error copying data
	 */
	private static void appendFile(OutputStream output, File source)
			throws IOException {
		InputStream input = null;
		try {
			input = new BufferedInputStream(new FileInputStream(source));
			IOUtils.copy(input, output);
		} finally {
			IOUtils.closeQuietly(input);
		}
	}

	/**
	 * Get a half cartesian - using the first part of the input RDD as a string key.
	 * @param <K> the type of the value
	 * @param <V> the type of the value
	 * @param inputRDD the input rdd - keys are 
	 * @return the RDD of all non-repated comparisons
	 */
	public static <K extends Comparable<K>,V> JavaPairRDD<Tuple2<K, V>, Tuple2<K, V>> getHalfCartesian(JavaPairRDD<K, V> inputRDD) {
		return getHalfCartesian(inputRDD, 0);
	}

	/**
	 * Get a half cartesian - using the first part of the input RDD as a string key.
	 * @param <K> the type of the value
	 * @param <V> the type of the value
	 * @param inputRDD the input rdd - keys are 
	 * @param numPartitions the number of partitions to repartition
	 * @return the RDD of all non-repated comparisons
	 */
	public static <K extends Comparable<K>,V> JavaPairRDD<Tuple2<K, V>, Tuple2<K, V>> getHalfCartesian(JavaPairRDD<K, V> inputRDD, int numPartitions) {
		JavaPairRDD<Tuple2<K, V>, Tuple2<K, V>> filteredRDD = inputRDD
				.cartesian(inputRDD)
				.filter(t -> t._1._1.compareTo(t._2._1)>0);
		// If there is partitions
		if (numPartitions!=0) {
			return filteredRDD.repartition(numPartitions);
		}
		// If not then just return it un re-partitoned
		return filteredRDD;

	}

	/**
	 * Generate a {@link JavaPairRDD} of String (PDB ID) and Value {@link StructureDataInterface} from a list of PDB ids
	 * @param pdbIdList the list of PDB ids
	 * @return the {@link JavaPairRDD} of String and {@link StructureDataInterface}
	 * @throws IOException due to reading from the URL
	 */
	public static JavaPairRDD<String, StructureDataInterface> getFromList(List<String> pdbIdList) throws IOException {
		// Load these structures
		List<Tuple2<String, byte[]>> totalList = new ArrayList<>();
		for(String pdbId : pdbIdList) {
			totalList.add(new Tuple2<String, byte[]>(pdbId, SparkUtils.getDataAsByteArray(pdbId)));
		}
		// Parrelise and return as RDD
		return SparkUtils.getSparkContext().parallelizePairs(totalList)
				.mapToPair(t -> new Tuple2<String, byte[]>(t._1.toString(), ReaderUtils.deflateGzip(t._2)))
				// Roughly a minute 
				.mapToPair(t -> new Tuple2<String, MmtfStructure>(t._1, new MessagePackSerialization().deserialize(new ByteArrayInputStream(t._2))))
				.mapToPair(t -> new Tuple2<String, StructureDataInterface>(t._1,  new GenericDecoder(t._2)));	

	}


	/**
	 * Get a {@link JavaRDD} from a {@link Dataset}.
	 * @param atomDataset the dataset to convert
	 * @param class1 the class of the dataset
	 * @param <T> the type of the dataset
	 * @return the {@link JavaRDD} fromn the dataset
	 */
	public static <T> JavaRDD<T> getJavaRdd(Dataset<T> atomDataset, Class<T> class1) {
		ClassTag<T> classTag = scala.reflect.ClassTag$.MODULE$.apply(class1);
		return new JavaRDD<T>(atomDataset.rdd(), classTag);
	}


	/**
	 * Parse the date as it is stored in the String.
	 * @param releaseDate the input date as a string
	 * @return the parsed date 
	 * @throws ParseException an error in parsing the date
	 */
	public static Date parseDate(String releaseDate) throws ParseException{
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		return formatter.parse(releaseDate);
	}
}