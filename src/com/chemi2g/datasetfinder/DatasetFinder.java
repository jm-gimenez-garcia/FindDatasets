package com.chemi2g.datasetfinder;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;

import org.apache.jena.atlas.web.HttpException;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;

public class DatasetFinder {

	static final String		LODLAUNDROMAT_ENDPOINT			= "http://sparql.backend.lodlaundromat.org";
	static final String		DATASET_URI_QUERY				= "SELECT ?url WHERE {<%s> <http://lodlaundromat.org/ontology/url> ?url}";
	static final String		DATASET_URI_QUERY_WITH_ARCHIVE	= "SELECT ?url WHERE {?archive <http://lodlaundromat.org/ontology/containsEntry> <%s> . ?archive <http://lodlaundromat.org/ontology/url> ?url}";

	HashMap<String, String>	dictionary						= new HashMap<String, String>();

	Date					date							= new Date();

	public static void main(String[] args) {
		DatasetFinder df = new DatasetFinder();
		df.readDictionary(new File(args[0]));
		df.processInput();
	}

	void readDictionary(File file) {
		String line;
		String[] fields;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			while ((line = reader.readLine()) != null) {
				fields = line.split(",");
				for (int i = 1; i < fields.length; i++) {
					dictionary.put(fields[1], fields[0]);
				}
			}
			reader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	void processInput() {
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(System.out));
		String input, resource, output;
		URL datasetUrl;
		try {
			while ((input = reader.readLine()) != null) {
				resource = input.split(" ")[1];
				if ((datasetUrl = query(String.format(DATASET_URI_QUERY, resource), LODLAUNDROMAT_ENDPOINT)) == null) {
					datasetUrl = query(String.format(DATASET_URI_QUERY_WITH_ARCHIVE, resource), LODLAUNDROMAT_ENDPOINT);
				}
				if (dictionary.containsKey(datasetUrl)) {
					output = "1," + dictionary.get(datasetUrl) + "," + datasetUrl + "\n";
				} else {
					output = "0," + dictionary.get(datasetUrl) + "," + datasetUrl + "\n";
				}
				writer.write(output);
			}
			writer.close();
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	URL query(String queryString, String endpoint) throws MalformedURLException, HttpException {
		URL url = null;
		try {
			Query query = QueryFactory.create(queryString);
			QueryExecution qexec = QueryExecutionFactory.sparqlService(endpoint, query);
			ResultSet results = qexec.execSelect();
			if (results.hasNext()) {
				url = new URL(results.next().getResource("url").toString());
			}
			qexec.close();
		} catch (Exception e) {
			System.err.println(new Timestamp(date.getTime()) + " Exception while querying SPARQL endpoint");
			e.printStackTrace();
			System.err.println(new Timestamp(date.getTime()) + " Resuming the process...");
		}
		return url;
	}

}
