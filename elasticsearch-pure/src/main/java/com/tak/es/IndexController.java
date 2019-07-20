/*
 * Package:  com.tak.elasticsearch
 * FileName: IndexController
 * Author:   Rust
 * Date:     19/5/18 20:23
 * email:    bryroy@gmail.com
 */
package com.tak.es;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * @author Rust
 */
@Controller
public class IndexController {

	@Resource
	private SpiderService spiderService;

	@GetMapping("greeting")
	public String greeting(@RequestParam(name = "name", required = false, defaultValue = "World") String name, Model model) {
		model.addAttribute("name", name);
		return "greeting";
	}

	@RequestMapping("test")
	public String test() {
		return "greeting";
	}


	@RequestMapping("")
	public String tesnt() {
		return "greeting";
	}


	@RequestMapping("search")
	public String search(@RequestParam String keyword, int num, int count, Model modelMap) {
		modelMap.addAttribute("Title", "振鑫花园人工智障搜索引擎");
		PageBean<HtmlBean> page = spiderService.search(keyword, num, count);
		modelMap.addAttribute("page", page);
		int divide = new BigDecimal(78).divide(new BigDecimal(10), RoundingMode.UP).intValue();
		int[] nums = new int[divide];
		for (int i = 0; i < nums.length; i++) {
			nums[i] = i + 1;
		}
		page.setNumbers(nums);
		// Double d = page.getTotalCount() / page.getSize();
		return "index";

	}

	public static void main(String[] args) {
		int divide = new BigDecimal(78).divide(new BigDecimal(10), RoundingMode.UP).intValue();
		System.out.println(divide);


	}

}
