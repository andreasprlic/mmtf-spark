package org.rcsb.mmtf.spark.utils;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.Map;

/**
 * 
 * A class to have a bi-directional map.
 * @author Anthony Bradley
 *
 *From here: http://stackoverflow.com/questions/3430170/how-to-create-a-2-way-map-in-java
 *
 * @param <K>
 * @param <V>
 */
public class TwoWayHashmap<K extends Object, V extends Object> implements Serializable {


	private static final long serialVersionUID = 7258753696587988372L;
	private Map<K,V> forward = new Hashtable<K, V>();
	private Map<V,K> backward = new Hashtable<V, K>();

	  /**
	   * Add a new key value pair
	   * @param key the key
	   * @param value the value
	   */
	  public synchronized void add(K key, V value) {
	    forward.put(key, value);
	    backward.put(value, key);
	  }

	  /**
	   * Get a value using a key in the conventional way.
	   * @param key the input key
	   * @return the value
	   */
	  public synchronized V getForward(K key) {
	    return forward.get(key);
	  }

	  /**
	   * Get a value using a key in the backward way.
	   * @param value the input value
	   * @return the key
	   */
	  public synchronized K getBackward(V value) {
	    return backward.get(value);
	  }
	}