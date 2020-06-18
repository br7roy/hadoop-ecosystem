/*
 * Package:  com.tak.elasticsearch
 * FileName: SpiderService
 * Author:   Tak
 * Date:     19/5/18 18:42
 * email:    bryroy@gmail.com
 */
package tk.tak.es;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author Tak
 */
@Service
public class SpiderService {

	public static final String DATA_DIR = "D:\\data";
	public Client client;

	public SpiderService() {
		try {
			Settings settings = Settings.builder().put("cluster.name", "es-cluster-1").build();
			client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName("s101"), 9300)).addTransportAddress(new TransportAddress(InetAddress.getByName("s102"), 9300)).addTransportAddress(new TransportAddress(InetAddress.getByName("s103"), 9300));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}


	/**
	 * 创建索引
	 * 每个field指定
	 * 是否分词，是否索引，是否保存
	 *
	 * @throws Exception
	 */
	public void createIndex() throws Exception {
		IndicesExistsResponse response = client.admin().indices().prepareExists("takno1").execute().actionGet();
		if (response.isExists()) {
			client.admin().indices().prepareDelete("takno1").execute().actionGet();
		}
		client.admin().indices().prepareCreate("takno1").execute().actionGet();
		XContentFactory factory = new XContentFactory();
		XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("htmlbean").startObject("properties")//type
				.startObject("title").field("type", "string").field("store", "yes").field("analyzer", "ik_max_word") //指定保存分詞
				.field("search_analyzer", "ik_max_word").endObject() //搜索字符串也需要分词
				.startObject("content").field("analyzer", "ik_max_word").field("search analyzer", "ik_max_word").endObject();
		PutMappingRequest mappingRequest = Requests.putMappingRequest("takno1").type("htmlbean").source(builder);
		client.admin().indices().putMapping(mappingRequest);

	}

	public void addHtmlToES() throws IOException {
		readHtml(new File(DATA_DIR));
	}

	private void readHtml(File file) throws IOException {
		Stream<Path> list = Files.walk(Paths.get(file.toURI()), Integer.MAX_VALUE);
		list.filter(r -> {
			return !Files.isDirectory(r);
		}).forEach(p -> {
			HtmlBean bean;
			try {
				bean = Utils.parse(p.toAbsolutePath().toString());
				if (bean == null) {
					return;
				}
				Map<String, String> dataMap = new HashMap<>();
				dataMap.put("title", bean.getTitle());
				dataMap.put("content", bean.getContent());
				dataMap.put("url", bean.getUrl());
				client.prepareIndex("takno1", "htmlbean").setSource(dataMap).execute().actionGet();
			} catch (Exception e) {
				e.printStackTrace();
			}

		});


	}


	/**
	 * 删除index
	 *
	 * @throws Exception
	 */
	public void deleteIndex() throws Exception {

		DeleteResponse response = client.prepareDelete("test4", "1", "1").get();
		System.out.println(response);


		// DeleteIndexResponse deletResp = client.admin().indices().prepareDelete("test3").execute().actionGet();
		// if (deletResp.isAcknowledged()) {


		// new DeleteIndexRequestBuilder(client, DeleteIndexAction.INSTANCE, "test3").execute(new ActionListener<AcknowledgedResponse>() {
		// 	public void onResponse(AcknowledgedResponse acknowledgedResponse) {
		// 		System.out.println(acknowledgedResponse);
		// 	}
		//
		// 	public void onFailure(Exception e) {
		// 		System.out.println("fail to delete");
		// 	}
		// });

		// }


	}

	public PageBean<HtmlBean> search(String text, int num, int count) {

		PageBean<HtmlBean> p = new PageBean<>();
		p.setIndex(num);


		// 多条件匹配
		MultiMatchQueryBuilder q = new MultiMatchQueryBuilder(text, "title", "content");
		SearchResponse response = null;
		if (p.getIndex() == 1) {
			HighlightBuilder builder = new HighlightBuilder();
			builder.field("title").field("content").preTags("<font color=\"red\"").postTags("</font>").fragmentSize(40)//设置显示结果中的一个碎片长度 词前后组成一个片段，总共40个字节
					.numOfFragments(5);//设置显示结果中每个结果最多显示碎片段，每个碎片段之间用...隔开
			response = client.prepareSearch("takno1").setTypes("htmlbean").setQuery(q).highlighter(builder).setFrom(0).setSize(10).execute().actionGet();


		} else {
			p.setTotalCount(count);
			HighlightBuilder builder = new HighlightBuilder();
			builder.field("title").field("content").preTags("<font color=\"red\"").postTags("</font>").fragmentSize(40)//设置显示结果中的一个碎片长度 词前后组成一个片段，总共40个字节
					.numOfFragments(5);//设置显示结果中每个结果最多显示碎片段，每个碎片段之间用...隔开

			response = client.prepareSearch("takno1")//
					.setTypes("htmlbean").setQuery(q)//
					.setFrom(p.getStartRow()).setSize(10).highlighter(builder).execute().actionGet();
		}
		SearchHits hits = response.getHits();
		p.setTotalCount((int) hits.getTotalHits().value);
		for (SearchHit hit : hits.getHits()) {
			HtmlBean bean = new HtmlBean();
			if (hit.getHighlightFields().get("title") == null) {
				bean.setTitle(hit.getFields().get("title").toString());
			} else {
				bean.setTitle(hit.getHighlightFields().get("title").getFragments()[0].toString());
			}
			if (hit.getHighlightFields().get("content") == null) {
				bean.setContent(hit.getFields().get("content").toString());
			} else {
				StringBuilder sb = new StringBuilder();
				for (Text tex : hit.getHighlightFields().get("content").getFragments()) {
					sb.append(tex.toString()).append("...");
				}
				bean.setContent(sb.toString());
			}
			bean.setUrl("http://" + hit.getSourceAsMap().get("url"));
			p.setBean(bean);
			p.getList().add(bean);
		}
		return p;
	}

	public static void main(String[] args) throws Exception {
		SpiderService spiderService = new SpiderService();
		// spiderService.deleteIndex();
		// spiderService.createIndex();
		spiderService.addHtmlToES();

	}
}
