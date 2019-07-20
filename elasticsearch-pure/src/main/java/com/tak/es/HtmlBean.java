/*
 * Package:  com.tak.elasticsearch
 * FileName: HtmlBean
 * Author:   Rust
 * Date:     19/5/18 18:17
 * email:    bryroy@gmail.com
 */
package com.tak.es;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author Rust
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
