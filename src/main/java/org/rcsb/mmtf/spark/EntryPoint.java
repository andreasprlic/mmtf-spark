package org.rcsb.mmtf.spark;

import py4j.GatewayServer;
import py4j.Py4JNetworkException;


/**
 * An entry point using Python to access Spark functions.
 * @author Anthony Bradley
 *
 */
public class EntryPoint {

	private SparkUtils sparkUtils;

	/**
	 * Constructor initialises the {@link SparkUtils} class.
	 */
	public EntryPoint() {
		// FIXME - can we get this to work as a Static class.
		sparkUtils = new SparkUtils();
	}

	/**
	 * Ability to get the SparkUtils.
	 * @return an instance of SparkUtils for Python to be used.
	 */
	public SparkUtils getSparkUtils() {
		return sparkUtils;
	}

	/**
	 * Function to set up the gateway server and get it going.
	 * @param args
	 */
	public static void main(String[] args) {
		GatewayServer gatewayServer = new GatewayServer(new EntryPoint());
		try{
			gatewayServer.start();
		}
		catch (Py4JNetworkException e){
			System.out.println("Gateway Server Already Started");
			return;
		}
		System.out.println("Gateway Server Started");
	}

}
