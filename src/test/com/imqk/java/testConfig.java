package com.imqk.java;
//import com.imqk.java.config.*;

import java.io.*;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.imqk.java.CONFIG;
import org.apache.commons.io.IOUtils;

/**
 * Created by 428900 on 2017/5/25.
 */
class JsonClass {
    // FOR JSON
    public String name;
    public String local;
}

public class testConfig {

    public static void main(String[] args) throws Exception{
        //config c = new config();
        System.out.println(Arrays.asList(CONFIG.strArray));
        System.out.println(CONFIG.name);


        //----------------------------------------------------
        System.out.println("\n另一个使用配置文件的方法：这个方法的属性，只能是String");

        Properties prop = new Properties();
        String filePath = testConfig.class.getClassLoader().getResource("config.properties").getPath();
        InputStream in = new BufferedInputStream(new FileInputStream(filePath));
        prop.load(in);
        Iterator<String> it = prop.stringPropertyNames().iterator();
        while(it.hasNext()) {
            String key = it.next();
            System.out.println(key + ": -->" + prop.getProperty(key));

        }

        in.close();

        //----------------------------------------------------
        ///保存属性到b.properties文件
        FileOutputStream oFile = new FileOutputStream("b.properties", true);//true表示追加打开
        prop.setProperty("phone", "10086");
        prop.store(oFile, "The New properties file");
        oFile.close();

        //----------------------------------------------------
        //使用JSON
        System.out.println("\n使用JSON作为配置文件");
        Gson gson = new Gson();
        FileInputStream json = null;
        try {
            String jsonPath = testConfig.class.getClassLoader().getResource("config.json").getPath();

            json = new FileInputStream(jsonPath);
            // 正式场合，应该给它另命名一个专门使用的类 testConfig
            JsonClass CONFIG = gson.fromJson(IOUtils.toString(json), JsonClass.class);

            // 访问到配置中的数据
            System.out.println("OBJ FROM JSON : " + CONFIG.name);

        } catch (JsonSyntaxException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(json);
        }
    }

}
