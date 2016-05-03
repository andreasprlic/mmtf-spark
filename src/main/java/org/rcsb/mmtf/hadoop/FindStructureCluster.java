package org.rcsb.mmtf.hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.vecmath.Point3d;

import org.biojava.nbio.structure.Structure;
import org.biojava.nbio.structure.StructureException;
import org.biojava.nbio.structure.StructureIO;
import org.biojava.nbio.structure.io.mmtf.MmtfStructureWriter;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.decoder.DefaultDecoder;
import org.rcsb.mmtf.decoder.ReaderUtils;
import org.rcsb.mmtf.encoder.AdapterToStructureData;

import scala.Tuple2;
import scala.Tuple5;

/**
 * Class to find the cluster fingerprint of a given structure.
 * @author Anthony Bradley
 *
 */
public class FindStructureCluster {


	private static Map<String, StructureDataInterface> TOTMAP = new HashMap<>();

	/**
	 * 
	 * @param args
	 * @throws IOException 
	 * @throws URISyntaxException 
	 * @throws JSONException 
	 * @throws ClassNotFoundException 
	 * @throws StructureException 
	 */
	public static void main(String[] args) throws IOException, JSONException, ClassNotFoundException, StructureException {

		for(String dom : getDomTot()) {
			try{
				TOTMAP.put(dom, getStrutureOfDom(dom));
			}
			catch(Exception e){
				System.out.println(dom+" not available!");
			}
		}
		System.out.println("GOT ALL DOMAINS!!!!");

		for(int fragmentLength : new int[]{3,4,5,6,7,8,9,10,11,12,13,14,15,20,30,40,50,75,100,150,200}){
			List<double[]> outVal = null;
			List<Tuple5<String, String, Double, Double, Double>> outList = new ArrayList<>();
			for(String domOne : getDomListOne()) {
				double bestScore = Double.MAX_VALUE;
				List<Double> answers = new ArrayList<>();
				String bestDomain  = "NA";
				for(String domTwo : getDomTot()) {
					if(!domOne.equals(domTwo)){
						double alignScore = calcAlign(domOne, domTwo, fragmentLength, outVal);
						answers.add(alignScore);
						if(alignScore<bestScore) {
							bestScore = alignScore;
							bestDomain = domTwo;
						}
					}
				}
				outList.add(new Tuple5<String, String, Double, Double, Double>(domOne, bestDomain, bestScore, getWorst(answers), getMean(answers)));
			}
			System.out.println(fragmentLength+" gives: "+outList.toString());
		}
	}


	private static StructureDataInterface getStrutureOfDom(String dom) throws IOException, StructureException {
		Structure biojavaStruct = StructureIO.getStructure(dom);
		AdapterToStructureData structureDataInterface = new AdapterToStructureData();
		new MmtfStructureWriter(biojavaStruct, structureDataInterface);
		return structureDataInterface;
	}


	private static Double getMean(List<Double> answers) {
		int numAns =0;
		double sumAns = 0.0;
		for(Double answer : answers) {
			if(answer.equals(Double.MAX_VALUE)){
				continue;
			}
			numAns++;
			sumAns+=answer;
		}
		return sumAns/numAns;
	}


	private static Double getWorst(List<Double> answers) {
		double worst = Double.MIN_VALUE;
		for(Double answer : answers) {
			if(answer.equals(Double.MAX_VALUE)){
				continue;
			}
			if(answer>worst) {
				worst = answer;
			}
		}
		return worst;
	}


	private static double calcAlign(String pdbOne, String pdbTwo, int fragmentLength, List<double[]> outVal) throws IOException {

		StructureDataInterface structOne = TOTMAP.get(pdbOne);
		StructureDataInterface structTwo = TOTMAP.get(pdbTwo);
		if(structOne==null || structTwo==null){
			return Double.MAX_VALUE;
		}
		long startTime = System.currentTimeMillis();
		double minDist = getMinDist(structOne, structTwo, fragmentLength);
		long endTime = System.currentTimeMillis();
//		System.out.println("Proccess took "+(endTime-startTime)+" ms.");	
		return minDist;
	}


	/**
	 * Function to find the optimal number of like clusters that can be aligned.
	 * @param fingerPrintOne the first fingerprint
	 * @param fingerPrint2 the second fingerprint
	 */
	private static double getAlignment(int[] fingerPrintOne, int[] fingerPrint2) {

		// Chose one to loop through and one to keep still
		int[] fingerPrintLoop;
		int[] fingerPrintStatic;
		// We want to chose the shortest one to loop through
		if (fingerPrintOne.length>fingerPrint2.length) {
			fingerPrintLoop = fingerPrint2;
			fingerPrintStatic = fingerPrintOne;
		}
		// Chose the other way round
		else{
			fingerPrintLoop = fingerPrintOne;
			fingerPrintStatic = fingerPrint2;
		}
		int maxScore = -1;
		int startIndex = -1;
		for(int i=0; i<fingerPrintLoop.length; i++) {
			// Start here and find the difference
			int score = 0;
			for(int j=0; j<fingerPrintLoop.length && j+i<fingerPrintStatic.length; j++){
				if(fingerPrintLoop[j]==fingerPrintStatic[j+i]){
					score++;
				}
			}
			if(score>maxScore){
				maxScore = score;
				startIndex = i;
			}
		}
		double alignment = maxScore*100/fingerPrintLoop.length;
		System.out.println(startIndex);
		System.out.println(alignment+" % alignment");
		return alignment;
	}


	/**
	 * Function to find the optimal number of like clusters that can be aligned.
	 * @param fingerPrintOne the first fingerprint
	 * @param fingerPrint2 the second fingerprint
	 * @throws IOException 
	 */
	private static double getMinDist(StructureDataInterface pdbId, StructureDataInterface pdbIdTwo, int fragLength) throws IOException {
		long startTime = System.currentTimeMillis();
		List<double[]> momentsOne = getMoments(pdbId, fragLength);
		List<double[]> momentsTwo = getMoments(pdbIdTwo, fragLength);
		long endTime = System.currentTimeMillis();
//		System.out.println(pdbId.getStructureId()+" "+pdbIdTwo.getStructureId());
//		System.out.println("Getting moments took "+(endTime-startTime)+" ms.");
		long startTimeTwo = System.currentTimeMillis();
		double minDist = getMinDist(momentsOne, momentsTwo, momentsOne.size()*4/5);
		long endTimeTwo = System.currentTimeMillis();
//		System.out.println("Getting min dist took "+(endTimeTwo-startTimeTwo)+" ms.");
		return minDist;
	}

	private static double getMinDist(List<double[]> fingerPrintLoop, List<double[]> fingerPrintStatic, int domainLen) {


		double minScore = Double.MAX_VALUE;
		for(int i=0; i<fingerPrintLoop.size(); i++) {
			// Start here and find the difference
			double score = 1.0;
			int len = 0;
			// 50 is the minm
			if(fingerPrintStatic.size()-i>domainLen){
				for(int j=0; j<fingerPrintLoop.size() && j+i<fingerPrintStatic.size(); j++){
					score+=getManDist(fingerPrintLoop.get(j),fingerPrintStatic.get(j+i));
					len++;
				}
			}
			// Normalise by length of overlap^2;	
			score = score/Math.pow(len,2.0);
			if(score<minScore){
				minScore = score;
			}
		}
		return minScore;
	}


	private static List<double[]> getMoments(StructureDataInterface structureDataInterface, int fragmentLength){
		List<Tuple2<String, Point3d[]>> fragments = new FragmentProteins(fragmentLength).getStructureAsFragments(structureDataInterface);
		List<double[]> momentsOut = new ArrayList<>();
		for (int i=0; i<fragments.size(); i++) {
			double[] moments = GenerateMoments.getMoments(fragments.get(i)._2);
			momentsOut.add(moments);
		}
		return momentsOut;
	}


	/**
	 * 
	 * @param pdbId
	 * @param fragmentLength
	 * @param outVal
	 * @return
	 * @throws IOException
	 */
	private static int[] getFingerPrint(Structure biojavaStruct, int fragmentLength, List<double[]> outVal) throws IOException {
		AdapterToStructureData structureDataInterface = new AdapterToStructureData();
		new MmtfStructureWriter(biojavaStruct, structureDataInterface);
		List<Tuple2<String, Point3d[]>> fragments = new FragmentProteins(fragmentLength).getStructureAsFragments(structureDataInterface);
		int[] fingerprint = new int[fragments.size()];
		for (int i=0; i<fragments.size(); i++) {
			double[] moments = GenerateMoments.getMoments(fragments.get(i)._2);
			int nearestClust = findNearest(moments, outVal);
			fingerprint[i] = nearestClust;
		}
		System.out.println(Arrays.toString(fingerprint));
		return fingerprint;
	}

	/**
	 * Find the nearest array from a list of arrays.
	 * @param moments the input array
	 * @param compArrays the list of comparison arrays
	 * @return the index of the closest array
	 */
	private static int findNearest(double[] moments, List<double[]> compArrays) {
		int nearest = -1;
		double dist = Double.MAX_VALUE;
		for(int i=0; i<compArrays.size(); i++){
			double thisDist = getManDist(compArrays.get(i), moments);
			if(thisDist<dist) {
				dist = thisDist;
				nearest = i;
			}
		}
		return nearest;
	}

	/**
	 * Find the manhattan distance between two arrays.
	 * @param arrayOne the first array
	 * @param arrayTwo the second array
	 * @return the Manhattan distance between the two arrays
	 */
	private static double getManDist(double[] arrayOne, double[] arrayTwo) {
		double sum = 0.0;
		for(int i=0; i<arrayOne.length; i++){
                        // Normalise the difference by one of the values
			sum += Math.abs(arrayOne[i]-arrayTwo[i]);
		}
		return sum;
	}



	/**
	 * 
	 * @param string
	 * @return
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	@SuppressWarnings("unchecked")
	private static List<double[]> readObjFile(String pathToClusterFile) throws IOException, ClassNotFoundException {
		System.out.println(FindStructureCluster.class.getClassLoader().getResource(pathToClusterFile).getPath().toString());
		File file = new File(FindStructureCluster.class.getClassLoader().getResource(pathToClusterFile).getFile());
		FileInputStream fileInputStream = new FileInputStream(file);
		ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
		List<double[]> outList = (List<double[]>) objectInputStream.readObject();
		objectInputStream.close();
		return outList;
	}

	/**
	 * Stupid utility method for reading a json into the clusters.
	 * @param pathToClusterFile
	 * @return
	 * @throws IOException
	 * @throws JSONException
	 */
	private static List<double[]> readJsonFromFile(String pathToClusterFile) throws IOException, JSONException {
		File file = new File(FindStructureCluster.class.getClassLoader().getResource(pathToClusterFile).getFile());
		FileInputStream fileInputStream = new FileInputStream(file);
		BufferedReader streamReader = new BufferedReader(new InputStreamReader(fileInputStream, "UTF-8")); 
		StringBuilder responseStrBuilder = new StringBuilder();
		String inputStr;
		while ((inputStr = streamReader.readLine()) != null)
			responseStrBuilder.append(inputStr);
		JSONObject jsonObject = new JSONObject(responseStrBuilder.toString());
		streamReader.close();

		JSONArray array = jsonObject.getJSONArray("VALS");
		List<double[]> outArr = new ArrayList<>();
		for(int i=0; i<array.length(); i++){
			JSONArray cluster = array.getJSONArray(i);
			double[] outClust = new double[cluster.length()];
			for(int j=0;j<cluster.length(); j++) {
				double val = cluster.getDouble(j);
				outClust[j] = val;
			}
			outArr.add(outClust);
		}
		return outArr;
	}


	private static String[] getDomListOne(){
		return new String[] {"d1r0ka3",
				"d7mdha2","d1hzpa1",
				"d1nw1a_",
				"d7mdha2",
				"d1mg7a1",
				"d1knxa2",
				"d1i8ta2",
				"d1s21a_",
				"d1r5ta_",
				"d1t3ta3",
				"d1poc__",
				"d1j09a1",
				"d1pcfa_",
				"d1h6ya_",
				"d1lxja_",
				"d1o7na1",
				"d1h2ba1",
				"d1ed1a_",
				"d1i8da2",
				"d1e0ta3",
				"d1jbwa1",
				"d1ro2a_",
				"d1pa1a_",
				"d1smye_",
				"d1mr1c_",
				"d1h6ta1",
				"d1e42a1",
				"d1grwa_",
				"d1nyna_",
				"d1s8na_",
				"d1bvyf_",
				"d1ug9a4",
				"d1umwa2",
				"d1s4ka_",
				"d1efdn_",
				"d1qgoa_",
				"d1rxwa2",
				"d1w1oa1",
				"d1hbnc_",
				"d1c02a_",
				"d1v4aa1",
				"d1bo1a_",
				"d1i9za_",
				"d1nyra2",
				"d1chua2",
				"d1lrza1",
				"d1guqa2",
				"d1rv9a_",
				"d1j9ia_",
				"d1irza_",
				"d1q1ha_",
				"d1l3la1",
				"d1oe4a_",
				"d1r0ka3",
				"d1p4da_",
				"d1fi4a2",
				"d1pzta_",
				"d1tf0b_",
				"d1ufaa1",
				"d1oqqa_",
				"d1m0wa1",
				"d1pvda2",
				"d1rwsa_",
				"d1e7la2",
				"d1am7a_",
				"d1pl3a_",
				"d1ow0a1",
				"d1u2ca1",
				"d1a4ya_",
				"d1m6ba1",
				"d1jeyb2",
				"d256ba_",
				"d1tzyc_",
				"d1l3ia_",
				"d1lbu_1",
				"d1p1ma1",
				"d1hfel2",
				"d1scjb_",
				"d2pii__",
				"d1koha2",
				"d1ct5a_",
				"d1mwva2",
				"d1un8a4",
				"d1s1qa_",
				"d1gl4a1",
				"d1fa0a1",
				"d1fnma4",
				"d1phza1",
				"d1v58a1",
				"d1jjcb5",
				"d1j31a_",
				"d1m3qa2",
				"d1fm4a_",
				"d1qhka_",
				"d1p90a_",
				"d1dt9a1",
				"d1woqa2",
				"d1nm8a1",
				"d1wmda2",
				"d1kp8a1",
				"d1axca2",
				"d1qwta_",
				"d1f8na1",
				"d1q5na_",
				"d1kr7a_",
				"d1nekb1",
				"d1nuia1",
				"d1jj2b_",
				"d1bu8a1",
				"d1k2fa_",
				"d1q67a_",
				"d1dfup_",
				"d1twia1",
				"d1usca_",
				"d1dkra1",
				"d1n3ka_",
				"d1t62a_",
				"d1rzhh1",
				"d1eyva_",
				"d1uf2c1",
				"d1vk1a_",
				"d1wdaa2",
				"d1lbu_2",
				"d1ml8a_",
				"d1k0ha_",
				"d1hska1",
				"d1ixh__",
				"d1uq5a_",
				"d1l7ya_",
				"d1c9fa_",
				"d1t5ya2",
				"d1sjwa_",
				"d1mola_",
				"d1qh5a_",
				"d1iata_",
				"d1uf3a_",
				"d1j6ra_",
				"d1b12a_",
				"d1koe__",
				"d1ir6a_",
				"d1q8ia2",
				"d1pu6a_",
				"d1rx0a2",
				"d1fjja_",
				"d1ak0__",
				"d1rl1a_",
				"d1flca1",
				"d1dqga_",
				"d1epwa2",
				"d1dfca4",
				"d1vl6a2",
				"d1aisb1",
				"d1ky9a1",
				"d1mxma1",
				"d1mvfd_",
				"d1cfza_",
				"d1jwqa_",
				"d1b24a2",
				"d1k8wa5",
				"d1smpi_",
				"d1lc5a_",
				"d1iwla_",
				"d1q77a_",
				"d1pfva2",
				"d1b65a_",
				"d1b93a_",
				"d1dv5a_",
				"d1okga3",
				"d1ngva_",
				"d2ilk__",
				"d1vkea_",
				"d1f7ta_",
				"d1pkp_2",
				"d1qvea_",
				"d1m70a2",
				"d1ve9a1",
				"d1m65a_",
				"d1hf8a1",
				"d1sr9a1",
				"d1ifya_",
				"d1iyka1",
				"d1tbba_",
				"d1jc4a_",
				"d1e19a_",
				"d1kid__",
				"d1kbla2",
				"d1orva2",
				"d1r5ja_",
				"d1d2ta_",
				"d1h6fa_",
				"d1amx__",
				"d1e5ca_",
				"d1rtu__",
				"d1hl9a1",
				"d1gyha_",
				"d1t0kb_",
				"d1pf5a_",
				"d1l0qa2",
				"d1a12a_",
				"d1u4ca_",
				"d1n3ja_",
				"d1k9ua_",
				"d1kzyc2",
				"d1ail__",
				"d1k7ca_",
				"d1gdha2",
				"d1p80a1",
				"d1tdta_",
				"d1qf6a3",
				"d1mbxc_",
				"d1uz5a3",
				"d1ltqa1",
				"d1jhga_",
				"d1l0oc_",
				"d1ae9a_",
				"d1k75a_",
				"d1ryaa_",
				"d1q32a2",
				"d1vhxa_",
				"d1njra_",
				"d1fiua_",
				"d1h7sa2",
				"d1ezfa_",
				"d1ujna_",
				"d1g3ka_",
				"d1okia1",
				"d1fp3a_",
				"d1cb8a1",
				"d1su8a_",
				"d5eau_1",
				"d1udda_",
				"d1ecma_",
				"d1dnv__",
				"d1ohfa_",
				"d1p2za1",
				"d1pgs_2",
				"d1ewna_",
				"d1prtd_",
				"d1oo9b_",
				"d1i50h_",
				"d1v9ta_",
				"d1g2913",
				"d1r0ua_",
				"d1js8a1",
				"d1ow1a_",
				"d1oewa_",
				"d1rl2a1",
				"d1ugpb_",
				"d1ub4a_",
				"d1g3sa3",
				"d1oz2a1",
				"d1kfta_",
				"d1v38a_",
				"d1szpa1",
				"d1q48a_",
				"d1d8ba_",
				"d1knya2",
				"d1jyha_",
				"d1fjgm_",
				"d2btva_",
				"d1nbwb_",
				"d1u14a_",
				"d1ihra_",
				"d1v2za_",
				"d1hxra_",
				"d1at0__",
				"d1onha_",
				"d1ni9a_",
				"d1uc8a2",
				"d1p16a2",
				"d1pn2a1",
				"d1xcba2",
				"d1r30a_",
				"d1u1ja1",
				"d1k87a2",
				"d1f6ya_",
				"d1j98a_",
				"d1jj2r_",
				"d1s0ya_",
				"d1oxwa_",
				"d1nbua_",
				"d1f5va_",
				"d1gqia2",
				"d1eb6a_",
				"d1ozha1",
				"d1oxxk2",
				"d1iznb_",
				"d1ep3b2",
				"d1siqa1",
				"d1niga_",
				"d1syya_",
				"d1vhka2",
				"d1vlpa1",
				"d1rlr_2",
				"d1nsj__",
				"d1o12a2",
				"d1edqa2",
				"d1ooya2",
				"d1cfya_",
				"d1tu1a_",
				"d1r4xa2",
				"d1kyha_",
				"d1gg4a4",
				"d1u0ka1",
				"d1k7ja_",
				"d1qb0a_",
				"d1o98a2",
				"d1lgr_2",
				"d1nzoa1",
				"d1bwda_",
				"d1woha_",
				"d1pgja1",
				"d1t0ga_",
				"d1gv9a_",
				"d1e0ta1",
				"d1rz4a2",
				"d1w3ba_",
				"d1szaa_",
				"d1azpa_",
				"d1v04a_",
				"d1e2wa2",
				"d1gpr__",
				"d1on3a2",
				"d1of9a_",
				"d1v16b2",
				"d2eng__",
				"d1ppya_",
				"d1q79a1",
				"d1h2ea_",
				"d1fjgd_",
				"d1ofla_",
				"d1vm0a_",
				"d1g6sa_",
				"d1gtfa_",
				"d1e5sa_",
				"d1cx4a2",
				"d1m4oa_",
				"d1nf1a_",
				"d1fs1b2",
				"d1gmia_",
				"d1a49a2",
				"d1r0ma1",
				"d1gvfa_",
				"d1lucb_",
				"d1tz9a_",
				"d1ccwb_",
				"d1qasa3",
				"d1nkga1",
				"d1p57a_",
				"d1bb8__",
				"d1jixa_",
				"d1ll8a_",
				"d1mkma2",
				"d1gw5s_",
		"d1a6ja_"};
	}

	private static String[] getDomTot() {

		return new String[] {"d1cjaa_","d1tqyb2","d1hzpa1","d1u8xx2",
				"d1nw1a_",
				"d1xeaa2",
				"d7mdha2",
				"d1mg7a1",
				"d1knxa2",
				"d1i8ta2",
				"d1s21a_",
				"d1r5ta_",
				"d1t3ta3",
				"d1poc__",
				"d1j09a1",
				"d1pcfa_",
				"d1h6ya_",
				"d1lxja_",
				"d1o7na1",
				"d1h2ba1",
				"d1ed1a_",
				"d1i8da2",
				"d1e0ta3",
				"d1jbwa1",
				"d1ro2a_",
				"d1pa1a_",
				"d1smye_",
				"d1mr1c_",
				"d1h6ta1",
				"d1e42a1",
				"d1grwa_",
				"d1nyna_",
				"d1s8na_",
				"d1bvyf_",
				"d1ug9a4",
				"d1umwa2",
				"d1s4ka_",
				"d1efdn_",
				"d1qgoa_",
				"d1rxwa2",
				"d1w1oa1",
				"d1hbnc_",
				"d1c02a_",
				"d1v4aa1",
				"d1bo1a_",
				"d1i9za_",
				"d1nyra2",
				"d1chua2",
				"d1lrza1",
				"d1guqa2",
				"d1rv9a_",
				"d1j9ia_",
				"d1irza_",
				"d1q1ha_",
				"d1l3la1",
				"d1oe4a_",
				"d1r0ka3",
				"d1p4da_",
				"d1fi4a2",
				"d1pzta_",
				"d1tf0b_",
				"d1ufaa1",
				"d1oqqa_",
				"d1m0wa1",
				"d1pvda2",
				"d1rwsa_",
				"d1e7la2",
				"d1am7a_",
				"d1pl3a_",
				"d1ow0a1",
				"d1u2ca1",
				"d1a4ya_",
				"d1m6ba1",
				"d1jeyb2",
				"d256ba_",
				"d1tzyc_",
				"d1l3ia_",
				"d1lbu_1",
				"d1p1ma1",
				"d1hfel2",
				"d1scjb_",
				"d2pii__",
				"d1koha2",
				"d1ct5a_",
				"d1mwva2",
				"d1un8a4",
				"d1s1qa_",
				"d1gl4a1",
				"d1fa0a1",
				"d1fnma4",
				"d1phza1",
				"d1v58a1",
				"d1jjcb5",
				"d1j31a_",
				"d1m3qa2",
				"d1fm4a_",
				"d1qhka_",
				"d1p90a_",
				"d1dt9a1",
				"d1woqa2",
				"d1nm8a1",
				"d1wmda2",
				"d1kp8a1",
				"d1axca2",
				"d1qwta_",
				"d1f8na1",
				"d1q5na_",
				"d1kr7a_",
				"d1nekb1",
				"d1nuia1",
				"d1jj2b_",
				"d1bu8a1",
				"d1k2fa_",
				"d1q67a_",
				"d1dfup_",
				"d1twia1",
				"d1usca_",
				"d1dkra1",
				"d1n3ka_",
				"d1t62a_",
				"d1rzhh1",
				"d1eyva_",
				"d1uf2c1",
				"d1vk1a_",
				"d1wdaa2",
				"d1lbu_2",
				"d1ml8a_",
				"d1k0ha_",
				"d1hska1",
				"d1ixh__",
				"d1uq5a_",
				"d1l7ya_",
				"d1c9fa_",
				"d1t5ya2",
				"d1sjwa_",
				"d1mola_",
				"d1qh5a_",
				"d1iata_",
				"d1uf3a_",
				"d1j6ra_",
				"d1b12a_",
				"d1koe__",
				"d1ir6a_",
				"d1q8ia2",
				"d1pu6a_",
				"d1rx0a2",
				"d1fjja_",
				"d1ak0__",
				"d1rl1a_",
				"d1flca1",
				"d1dqga_",
				"d1epwa2",
				"d1dfca4",
				"d1vl6a2",
				"d1aisb1",
				"d1ky9a1",
				"d1mxma1",
				"d1mvfd_",
				"d1cfza_",
				"d1jwqa_",
				"d1b24a2",
				"d1k8wa5",
				"d1smpi_",
				"d1lc5a_",
				"d1iwla_",
				"d1q77a_",
				"d1pfva2",
				"d1b65a_",
				"d1b93a_",
				"d1dv5a_",
				"d1okga3",
				"d1ngva_",
				"d2ilk__",
				"d1vkea_",
				"d1f7ta_",
				"d1pkp_2",
				"d1qvea_",
				"d1m70a2",
				"d1ve9a1",
				"d1m65a_",
				"d1hf8a1",
				"d1sr9a1",
				"d1ifya_",
				"d1iyka1",
				"d1tbba_",
				"d1jc4a_",
				"d1e19a_",
				"d1kid__",
				"d1kbla2",
				"d1orva2",
				"d1r5ja_",
				"d1d2ta_",
				"d1h6fa_",
				"d1amx__",
				"d1e5ca_",
				"d1rtu__",
				"d1hl9a1",
				"d1gyha_",
				"d1t0kb_",
				"d1pf5a_",
				"d1l0qa2",
				"d1a12a_",
				"d1u4ca_",
				"d1n3ja_",
				"d1k9ua_",
				"d1kzyc2",
				"d1ail__",
				"d1k7ca_",
				"d1gdha2",
				"d1p80a1",
				"d1tdta_",
				"d1qf6a3",
				"d1mbxc_",
				"d1uz5a3",
				"d1ltqa1",
				"d1jhga_",
				"d1l0oc_",
				"d1ae9a_",
				"d1k75a_",
				"d1ryaa_",
				"d1q32a2",
				"d1vhxa_",
				"d1njra_",
				"d1fiua_",
				"d1h7sa2",
				"d1ezfa_",
				"d1ujna_",
				"d1g3ka_",
				"d1okia1",
				"d1fp3a_",
				"d1cb8a1",
				"d1su8a_",
				"d5eau_1",
				"d1udda_",
				"d1ecma_",
				"d1dnv__",
				"d1ohfa_",
				"d1p2za1",
				"d1pgs_2",
				"d1ewna_",
				"d1prtd_",
				"d1oo9b_",
				"d1i50h_",
				"d1v9ta_",
				"d1g2913",
				"d1r0ua_",
				"d1js8a1",
				"d1ow1a_",
				"d1oewa_",
				"d1rl2a1",
				"d1ugpb_",
				"d1ub4a_",
				"d1g3sa3",
				"d1oz2a1",
				"d1kfta_",
				"d1v38a_",
				"d1szpa1",
				"d1q48a_",
				"d1d8ba_",
				"d1knya2",
				"d1jyha_",
				"d1fjgm_",
				"d2btva_",
				"d1nbwb_",
				"d1u14a_",
				"d1ihra_",
				"d1v2za_",
				"d1hxra_",
				"d1at0__",
				"d1onha_",
				"d1ni9a_",
				"d1uc8a2",
				"d1p16a2",
				"d1pn2a1",
				"d1xcba2",
				"d1r30a_",
				"d1u1ja1",
				"d1k87a2",
				"d1f6ya_",
				"d1j98a_",
				"d1jj2r_",
				"d1s0ya_",
				"d1oxwa_",
				"d1nbua_",
				"d1f5va_",
				"d1gqia2",
				"d1eb6a_",
				"d1ozha1",
				"d1oxxk2",
				"d1iznb_",
				"d1ep3b2",
				"d1siqa1",
				"d1niga_",
				"d1syya_",
				"d1vhka2",
				"d1vlpa1",
				"d1rlr_2",
				"d1nsj__",
				"d1o12a2",
				"d1edqa2",
				"d1ooya2",
				"d1cfya_",
				"d1tu1a_",
				"d1r4xa2",
				"d1kyha_",
				"d1gg4a4",
				"d1u0ka1",
				"d1k7ja_",
				"d1qb0a_",
				"d1o98a2",
				"d1lgr_2",
				"d1nzoa1",
				"d1bwda_",
				"d1woha_",
				"d1pgja1",
				"d1t0ga_",
				"d1gv9a_",
				"d1e0ta1",
				"d1rz4a2",
				"d1w3ba_",
				"d1szaa_",
				"d1azpa_",
				"d1v04a_",
				"d1e2wa2",
				"d1gpr__",
				"d1on3a2",
				"d1of9a_",
				"d1v16b2",
				"d2eng__",
				"d1ppya_",
				"d1q79a1",
				"d1h2ea_",
				"d1fjgd_",
				"d1ofla_",
				"d1vm0a_",
				"d1g6sa_",
				"d1gtfa_",
				"d1e5sa_",
				"d1cx4a2",
				"d1m4oa_",
				"d1nf1a_",
				"d1fs1b2",
				"d1gmia_",
				"d1a49a2",
				"d1r0ma1",
				"d1gvfa_",
				"d1lucb_",
				"d1tz9a_",
				"d1ccwb_",
				"d1qasa3",
				"d1nkga1",
				"d1p57a_",
				"d1bb8__",
				"d1jixa_",
				"d1ll8a_",
				"d1mkma2",
				"d1gw5s_",
				"d1a6ja_",
				"d1tqyb2",
				"d1cjaa_",
				"d1u8xx2",
				"d1kkha1",
				"d1os1a1",
				"d1f8ra2",
				"d1a26_2",
				"d1p6oa_",
				"d1gtda_",
				"d1lwba_",
				"d1irxa1",
				"d1l3aa_",
				"d1od3a_",
				"d1s7ha_",
				"d1nyka_",
				"d1we3o_",
				"d1uhua_",
				"d1ja1a1",
				"d1t57a_",
				"d1gg4a1",
				"d1v33a_",
				"d1lw3a2",
				"d1i50f_",
				"d1ufna_",
				"d1p7ba1",
				"d1r4xa1",
				"d1klfa1",
				"d1t95a2",
				"d1qo0d_",
				"d1d4aa_",
				"d1qwna2",
				"d1mbya_",
				"d1adr__",
				"d1m1nb_",
				"d1doza_",
				"d1v8pa_",
				"d1diqa1",
				"d1hbnb2",
				"d1i5na_",
				"d1knya1",
				"d1obda_",
				"d1ako__",
				"d1jala2",
				"d1trb_2",
				"d1eiya1",
				"d1fit__",
				"d1hq0a_",
				"d1xpa_1",
				"d1ntca_",
				"d1fnna1",
				"d1fc3a_",
				"d1ui0a_",
				"d1xeaa2",
				"d1r9wa_",
				"d1mg7a2",
				"d1g1la_",
				"d1lp1a_",
				"d1qwna1",
				"d1kf6b2",
				"d1gsoa2",
				"d1keka1",
				"d1fm0d_",
				"d1v0da_",
				"d1k28a3",
				"d1i8aa_",
				"d1nkoa_",
				"d1l3wa4",
				"d1yrga_",
				"d1ozna_",
				"d1jeya2",
				"d2ccya_",
				"d1tafb_",
				"d1g38a_",
				"d1slm_1",
				"d1k6wa1",
				"d1xer__",
				"d1jqga2",
				"d1naqa_",
				"d1uw4a_",
				"d1vfsa2",
				"d1q4ga1",
				"d1mgpa_",
				"d1ttea2",
				"d1oxda_",
				"d1r89a3",
				"d1t95a3",
				"d1q5ya_",
				"d1a8l_1",
				"d1bia_3",
				"d1emsa2",
				"d1aisa1",
				"d1kcma_",
				"d1div_2",
				"d1o13a_",
				"d1fjgk_",
				"d1huxa_",
				"d1q9ja2",
				"d1ga6a_",
				"d1q3qa1",
				"d1ok7a2",
				"d1mjsa_",
				"d1lox_1",
				"d1gkma_",
				"d1dlwa_",
				"d1h7wa1",
				"d1dd9a_",
				"d1r5ba1",
				"d1f8na2",
				"d1czya1",
				"d1gg3a2",
				"d1gtra1",
				"d1rcqa1",
				"d1vl7a_",
				"d1hgxa_",
				"d1pn5a1",
				"d1t5ya1",
				"d1pm3a_",
				"d1q8ca_",
				"d1qhda1",
				"d1vz0a2",
				"d1jh6a_",
				"d1ikop_",
				"d1tzpa_",
				"d1nyed_",
				"d1k28d2",
				"d1jroa4",
				"d1ryoa_",
				"d1r4pa_",
				"d1i42a_",
				"d1oeyj_",
				"d1iq8a4",
				"d1hkxa_",
				"d1roaa_",
				"d1vjna_",
				"d1vima_",
				"d4kbpa2",
				"d1msk__",
				"d1f39a_",
				"d1prea1",
				"d1k20a_",
				"d1jiha2",
				"d1lmza_",
				"d1is2a3",
				"d1qoua_",
				"d1ca1_1",
				"d1shsa_",
				"d2viua_",
				"d1knma_",
				"d1avac_",
				"d1hcd__",
				"d1hwxa2",
				"d1guxb_",
				"d1ujua_",
				"d1hk9a_",
				"d1n0ea_",
				"d1c8ba_",
				"d1lam_2",
				"d1dq3a4",
				"d1dj0a_",
				"d1oh1a_",
				"d1pmma_",
				"d1iwma_",
				"d1ni5a1",
				"d1g8fa2",
				"d1vz6a_",
				"d1a9xa2",
				"d1dnya_",
				"d1grj_2",
				"d1jw9b_",
				"d1v7mv_",
				"d1knca_",
				"d1qr0a2",
				"d1kn0a_",
				"d1oqva_",
				"d1qksa1",
				"d1i8ta1",
				"d1v77a_",
				"d1j2jb_",
				"d1rqba1",
				"d1otra_",
				"d1sqha_",
				"d1vj7a1",
				"d1u69a_",
				"d1gs5a_",
				"d1q3qa2",
				"d1zyma2",
				"d1c4xa_",
				"d1ptma_",
				"d1up8a_",
				"d1mnna_",
				"d1p5vb_",
				"d1qba_2",
				"d1aqza_",
				"d1g5aa1",
				"d1oyga_",
				"d1dt9a2",
				"d1ufya_",
				"d1pbyb_",
				"d1jtdb_",
				"d1p22a2",
				"d1p0ya2",
				"d1sra__",
				"d1in1a_",
				"d1a32__",
				"d1esc__",
				"d1l7da2",
				"d1kwga3",
				"d1j2za_",
				"d1v7oa_",
				"d1ctf__",
				"d1uuya_",
				"d1zrn__",
				"d1j1va_",
				"d1s7oa_",
				"d1k4ta2",
				"d1euha_",
				"d1q27a_",
				"d1v0wa1",
				"d1ekea_",
				"d1lam_1",
				"d1xhva_",
				"d1jm6a2",
				"d1n1ba2",
				"d1kq3a_",
				"d2pvaa_",
				"d1f53a_",
				"d1ks8a_",
				"d1qaza_",
				"d1gnla_",
				"d1n4qb_",
				"d1j77a_",
				"d5csma_",
				"d1lp3a_",
				"d1ihma_",
				"d1hx6a1",
				"d1sdwa2",
				"d1s3ia1",
				"d1eu3a1",
				"d1jb3a_",
				"d1k3ra1",
				"d1x7fa1",
				"d1fr3a_",
				"d1kxoa_",
				"d1bt3a_",
				"d1jeyb1",
				"d4fiv__",
				"d1iz6a1",
				"d1jb0e_",
				"d3vub__",
				"d1igqa_",
				"d1ssfa1",
				"d1dgsa1",
				"d1ji7a_",
				"d1u9la_",
				"d1mzga_",
				"d1go3f_",
				"d1q79a2",
				"d1d5ya3",
				"d1tdza1",
				"d1uf2a_",
				"d1eexb_",
				"d1v7ra_",
				"d1lr0a_",
				"d1w53a_",
				"d1txja_",
				"d1mi8a_",
				"d1u60a_",
				"d1jp4a_",
				"d1m0wa2",
				"d1fvia2",
				"d1psua_",
				"d1vlla_",
				"d1tv8a_",
				"d1r3sa_",
				"d1b5ta_",
				"d1eyea_",
				"d1hr6a2",
				"d1jj2l_",
				"d1mwwa_",
				"d1nm2a1",
				"d1j2ga1",
				"d1vkwa_",
				"d1nowa2",
				"d1k9xa_",
				"d1j8fa_",
				"d1d0ya2",
				"d1izna_",
				"d1krha2",
				"d1is2a2",
				"d1noga_",
				"d1lb3a_",
				"d1oy5a_",
				"d1o4ua2",
				"d1hk8a_",
				"d1tqja_",
				"d1a4ma_",
				"d1fcqa_",
				"d1vl1a_",
				"d1svy__",
				"d1v2ba_",
				"d1e42a2",
				"d1v8aa_",
				"d1jbwa2",
				"d1gqza2",
				"d1snna_",
				"d1okga2",
				"d1ei6a_",
				"d1crka2",
				"d1tvfa1",
				"d1wdaa3",
				"d1c3pa_",
				"d1ks9a1",
				"d1kbia2",
				"d1ms9a1",
				"d1o65a_",
				"d1e7ua1",
				"d1hz4a_",
				"d1eyha_",
				"d1pfba_",
				"d1pjxa_",
				"d1b6ra1",
				"d1qwya_",
				"d1tyfa_",
				"d1l9la_",
				"d1l8aa3",
				"d1n10a2",
				"d1g8ka1",
				"d1px5a1",
				"d1bif_2",
				"d1vioa2",
				"d1h80a_",
				"d1udva_",
				"d1qmha2",
				"d1pg6a_",
				"d1nx4a_",
				"d1omia1",
				"d1o5ua_",
				"d1pbwa_",
				"d1t1da_",
				"d3rpba_",
				"d1m3ua_",
				"d1onea1",
				"d1vlia2",
				"d1ezwa_",
				"d1k77a_",
				"d7reqb1",
				"d1o1za_",
				"d1kul__",
				"d1by2__",
				"d1kjka_",
				"d1uqta_",
				"d1ew0a_",
				"d1stza2",
				"d1h8ma_",
		"d1hynp_"};
	}

}
