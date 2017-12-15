package com.sxt.vo;

import net.htmlparser.jericho.Element;
import net.htmlparser.jericho.HTMLElementName;
import net.htmlparser.jericho.Source;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * Created by Administrator on 2016/1/15.
 */
public class HtmlBeanUtil {

    public static HtmlBean createBean(File file){
        HtmlBean hb = new HtmlBean();
        try {
            Source sc = new Source(file);
            Element element = sc.getFirstElement(HTMLElementName.TITLE);
            if(element==null){
                return null;
            }
            hb.setTitle(element.getTextExtractor().toString());
            hb.setContext(sc.getTextExtractor().toString());
            String path = file.getAbsolutePath();
            //Â·¾¶ÊÇG:\www.bjsxt.com
            hb.setUrl("http://" + path.substring(3));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return hb;

    }
}
