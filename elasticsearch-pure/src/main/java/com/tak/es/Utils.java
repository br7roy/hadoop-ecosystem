/*
 * Package:  com.tak.elasticsearch
 * FileName: Utils
 * Author:   Rust
 * Date:     19/5/18 18:27
 * email:    bryroy@gmail.com
 */
package com.tak.es;

import net.htmlparser.jericho.CharacterReference;
import net.htmlparser.jericho.Element;
import net.htmlparser.jericho.HTMLElementName;
import net.htmlparser.jericho.Source;

import java.io.File;

import static com.tak.es.SpiderService.DATA_DIR;


/**
 * @author Rust
 */
public class Utils {

	/**
	 * 两种选择要么另外建项目排除了以来问题
	 * 要么使用spring-data-es集成
	 * https://blog.csdn.net/chen_2890/article/details/83895646
	 *
	 * @param path
	 * @return
	 * @throws Exception
	 */
	public static HtmlBean parse(String path) throws Exception {
		HtmlBean bean = new HtmlBean();
		Source source = new Source(new File(path));
		source.fullSequentialParse();
		Element titleEle = source.getFirstElement(HTMLElementName.TITLE);
		if (titleEle == null) {
			return null;
		} else {
			String title = CharacterReference.decodeCollapseWhiteSpace(titleEle.getContent());
			bean.setTitle(title);
		}
		String content = source.getTextExtractor().setIncludeAttributes(true).toString();
		String url = path.substring(DATA_DIR.length());
		bean.setContent(content);
		bean.setUrl(url);
		return bean;
	}
}
