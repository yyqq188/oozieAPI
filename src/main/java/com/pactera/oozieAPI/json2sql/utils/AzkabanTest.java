package com.pactera.oozieAPI.json2sql.utils;

import java.io.IOException;

public class AzkabanTest {
  public void run(String[] args) throws IOException {
        // 根据需求编写具体代码
      for (String arg : args) {
        System.out.println("hello azkaban args:"+arg);
      }
    }


  public static void main(String[] args) throws IOException {
      AzkabanTest azkabanTest = new AzkabanTest();
      azkabanTest.run(args);
  }
}
