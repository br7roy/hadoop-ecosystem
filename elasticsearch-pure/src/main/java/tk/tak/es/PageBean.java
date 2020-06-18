/*
 * Package:  com.tak.elasticsearch
 * FileName: PageBean
 * Author:   Tak
 * Date:     19/5/18 21:51
 * email:    bryroy@gmail.com
 */
package tk.tak.es;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Tak
 */
@Getter
@Setter
public class PageBean<T> {

	private int size = 10;//每页显示记录
	private int index = 1;//当前页号
	private int totalCount = 0;//记录总数

	private int totalPageCount = 1;//总页
	private int[] numbers;//展示页数集合
	protected List<T> list;//要显示到页面的数据集
	private int startRow;
	private HtmlBean bean;

	public PageBean() {
		list = new ArrayList<>();
	}

	public int getStartRow() {
		return (index - 1) * size;
	}

	public int getEndRow() {
		return index * size;
	}


}
