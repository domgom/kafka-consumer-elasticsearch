package com.thomascook.kafkaconsumer.format;

import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;

public class ElasticSearchSchemaFormateer {

	private static final String SEPARATOR = "\\|@\\|@\\|@\\|";

	public static void write(Map<String, Object> json, String msg, long offset) {

		String[] pieces = msg.split(SEPARATOR);
		if (pieces.length == 2) {
			json.put("message", pieces[1]);
			propagateHeadersAsIndexedFields(json, pieces);
		}else{
			json.put("UnformattedMessage", msg);
		}
		json.put("offset", offset);
	}

	private static void propagateHeadersAsIndexedFields(Map<String, Object> json, String[] pieces) {
		Gson gson = new Gson();
		HashMap<?, ?> headers = gson.fromJson(pieces[0], HashMap.class);

		for (Object key : headers.keySet()) {
			json.put(key.toString(), headers.get(key));
		}
	}

}
