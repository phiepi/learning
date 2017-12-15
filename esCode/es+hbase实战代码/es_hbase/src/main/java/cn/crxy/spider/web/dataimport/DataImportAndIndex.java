package cn.crxy.spider.web.dataimport;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import cn.crxy.spider.web.domain.Article;
import cn.crxy.spider.web.utils.Esutil;
import cn.crxy.spider.web.utils.HbaseUtils;

public class DataImportAndIndex {
	
	public static void main(String[] args) throws java.lang.Exception {
		List<Article> arrayList = new ArrayList<Article>();
		//TODO 从其他数据源读取数据
		File file = new File("C:\\Users\\Administrator\\Desktop\\article.txt");
		if(!file.exists()){
			System.err.println("文件不存在！");
		}
		InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream(file), "UTF-8");
		
		BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
		String lineString = null;
		while((lineString = bufferedReader.readLine())!=null){
			Article article = new Article();
			String[] split = lineString.split("\t");
			int parseInt = Integer.parseInt(split[0].trim());
			article.setId(parseInt);
			article.setTitle(split[1]);
			article.setAuthor(split[2]);
			article.setDescribe(split[3]);
			article.setContent(split[3]);
			arrayList.add(article);
		}
		HbaseUtils hbaseUtils = new HbaseUtils();
		for (Article article : arrayList) {
			try {
				//把数据插入hbase
				hbaseUtils.put(hbaseUtils.TABLE_NAME, article.getId()+"", hbaseUtils.COLUMNFAMILY_1, hbaseUtils.COLUMNFAMILY_1_TITLE, article.getTitle());
				hbaseUtils.put(hbaseUtils.TABLE_NAME, article.getId()+"", hbaseUtils.COLUMNFAMILY_1, hbaseUtils.COLUMNFAMILY_1_AUTHOR, article.getAuthor());
				hbaseUtils.put(hbaseUtils.TABLE_NAME, article.getId()+"", hbaseUtils.COLUMNFAMILY_1, hbaseUtils.COLUMNFAMILY_1_DESCRIBE, article.getDescribe());
				hbaseUtils.put(hbaseUtils.TABLE_NAME, article.getId()+"", hbaseUtils.COLUMNFAMILY_1, hbaseUtils.COLUMNFAMILY_1_CONTENT, article.getContent());
				//把数据插入es
				Esutil.addIndex("crxy","article",article);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		inputStreamReader.close();
		bufferedReader.close();
	}

}
