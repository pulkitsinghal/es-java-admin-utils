package com.fermyon.test.elasticsearch.jobs;

import io.searchbox.Action;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.ClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Bulk.Builder;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchScroll;
import io.searchbox.params.Parameters;
import io.searchbox.params.SearchType;

import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;

/**
 * This is a utility that fills in the implementation gap for the "reindexing" step outlined in:
 * 
 * http://www.elasticsearch.org/blog/changing-mapping-with-zero-downtime/
 * 
 * @author pulkitsinghal
 */
public class ReindexData extends TestCase {

	public static String FROM_INDEX = "from_index_name";
	public static String TO_INDEX = "to_index_name";
	public static String TYPE = "your_type"; //optional
	public static String ES_URL = "https://username:password@your.esinstance.com:9443";
	public static int PAGE_SIZE = 1000;

	@Test
	public void reindex()
	{
		// Configuration
		ClientConfig clientConfig = new ClientConfig.Builder(ES_URL).multiThreaded(false).build();
		// Construct a new Jest client according to configuration via factory
		JestClientFactory factory = new JestClientFactory();
		factory.setClientConfig(clientConfig);
		JestClient client = factory.getObject();

		QueryBuilder onlyReindexDocsFromthisQuery = QueryBuilders.matchAllQuery();
		//QueryBuilder query = QueryBuilders.matchQuery("someField", "someValue"); // optional

		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(onlyReindexDocsFromthisQuery);

		Search search = new Search
				.Builder(searchSourceBuilder.toString())
				.addIndex(FROM_INDEX)
				.addType(TYPE)
				.setParameter(Parameters.SEARCH_TYPE, SearchType.SCAN)
				.setParameter(Parameters.SIZE, PAGE_SIZE)
				.setParameter(Parameters.SCROLL, "5m")
				.build();
		System.out.println(search.getData(null));
		JestResult result = handleResult(client, search);
		String scrollId = result.getJsonObject().get("_scroll_id").getAsString();

		int currentResultSize = 0;
		int pageNumber = 1;
		do {
			SearchScroll scroll = new SearchScroll.Builder(scrollId, "5m").build();
			result = handleResult(client, scroll);
			scrollId = result.getJsonObject().get("_scroll_id").getAsString();
			List hits = ((List) ((Map) result.getJsonMap().get("hits")).get("hits"));
			currentResultSize = hits.size();
			System.out.println("finished scrolling page # " + pageNumber++ + " which had " + currentResultSize + " results.");

			boolean somethingToIndex = false;
			Builder bulkIndexBuilder = new Bulk
					.Builder()
					.defaultIndex(TO_INDEX)
					.defaultType(TYPE);
			for (int i = 0; i < currentResultSize; i++) {
				Map source = ((Map) ((Map) hits.get(i)).get("_source"));
				String sourceId = ((String) ((Map) hits.get(i)).get("_id"));
				System.out.println("adding " + sourceId + " for bulk indexing");
				Index index = new Index.Builder(source)
				.index(TO_INDEX)
				.type(TYPE)
				.id(sourceId)
				.build();
				bulkIndexBuilder = bulkIndexBuilder.addAction(index);
				somethingToIndex = true;
			}
			if (somethingToIndex) {
				Bulk bulk = bulkIndexBuilder.build();
				//System.out.println(bulk.getData(null)); // results in NullPointerException
				handleResult(client, bulk);
			} else {
				System.out.println("there weren't any results to index in this set/page");
			}
		} while (currentResultSize == PAGE_SIZE);

		System.out.println("************************");
	}

	private JestResult handleResult (JestClient client, Action action) {
		JestResult result = null;
		try {
			result = client.execute(action);
			if (result.isSucceeded()) {
				System.out.println(result.getJsonString());
				//List hits = ((List) ((Map) result.getJsonMap().get("hits")).get("hits"));
				//System.out.println("hits.size(): " + hits.size());
			} else {
				System.out.println(result.getErrorMessage());
				System.out.println(result.getJsonString());
				System.exit(0);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
		return result;
	}
}