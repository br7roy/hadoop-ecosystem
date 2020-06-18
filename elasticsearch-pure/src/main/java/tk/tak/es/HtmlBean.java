/*
 * Package:  com.tak.elasticsearch
 * FileName: HtmlBean
 * Author:   Tak
 * Date:     19/5/18 18:17
 * email:    bryroy@gmail.com
 */
package tk.tak.es;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author Tak
 */
@Getter
@Setter
@ToString
public class HtmlBean {
	private int id;
	private String title;
	private String content;
	private String url;
}
