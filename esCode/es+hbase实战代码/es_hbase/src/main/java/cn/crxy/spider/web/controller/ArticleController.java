package cn.crxy.spider.web.controller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import cn.crxy.spider.web.domain.Article;
import cn.crxy.spider.web.page.Page;
import cn.crxy.spider.web.utils.Esutil;
import cn.crxy.spider.web.utils.FreeMarkerUtil;
import cn.crxy.spider.web.utils.HbaseUtils;

@Controller
@RequestMapping("article")
public class ArticleController {
	static final Logger logger = LoggerFactory.getLogger(ArticleController.class);
	private static final String ARTICLE_INDEX = "/article/articleList";
	private static final String ARTICLE_DETAIL = "/article/articleDetail";
	HbaseUtils hbaseUtils = new HbaseUtils();

	@RequestMapping("")
	public String index(ModelMap modelMap){
		
		return ARTICLE_INDEX;
	}
	
	/**
	 * 搜索
	 * @param modelMap
	 * @param skey
	 * @param sort
	 * @param request
	 * @param start
	 * @param row
	 * @return
	 */
	@ResponseBody
	@RequestMapping(value="/search", produces = "application/text; charset=utf-8")
	public String serachArticle(ModelMap modelMap,
			@RequestParam(value="skey",required = false) String skey,
			HttpServletRequest request,
			@RequestParam(value = "start", defaultValue = "0") Integer start,
			@RequestParam(value = "row", defaultValue = "10") Integer row){
		Map<String,Object> map = new HashMap<String, Object>();
		Long count = 0L;
		try {
			map = Esutil.search(skey,"crxy","article",start, row);
			count = (Long)map.get("count");
		} catch (Exception e) {
			logger.error("查询索引错误!{}",e);
			e.printStackTrace();
		}
		List<Map<String, Object>> articleList = (List<Map<String, Object>>)map.get("dataList");
		
		String nextUrl = request.getContextPath() + "/article"+"/search";
		String pageString = Page.getJsonPage(request, "", start, row, count,nextUrl);
		Map<String, Object> root = new HashMap<String, Object>();
		root.put("pageString", pageString);
		root.put("articleList", articleList);
		root.put("count", count);
		root.put("row", row);
		root.put("contentPath", request.getContextPath());
		String result = FreeMarkerUtil.parseTemplate(
				"article/articlelist.ftl", root);
		modelMap.put("count", count);
		return result;
	}

	
	
	
	

	

	
	/**
	 * 查看文章详细信息
	 * @return
	 */
	@RequestMapping("/detailArticleById/{id}")
	public String detailArticleById(@PathVariable(value="id") String id,ModelMap modelMap){
		Article article = hbaseUtils.get(hbaseUtils.TABLE_NAME, id);
		modelMap.put("article", article);
		return ARTICLE_DETAIL;
	}
	
	
	
}
