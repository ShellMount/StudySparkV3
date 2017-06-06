package com.imqk.jvm;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by 428900 on 2017/5/25.

   堆栈益出模拟
   JVM 运行参数：
   -verbose:gc -Xms10M -Xms10M -XX:MaxDirectMemorySize=5M -Xss128 -XX:+PrintGCDetails
*/

class Person{
    private String[] p = {"AAAAAAAAAAA", "BBBBBBBBB", "CCCCCCCCC"};
}

public class J01_HeapStack {
    public static void main (String[] args) {
        System.out.println("HelloHeapOutOfMemory");
        List<Person> persons = new ArrayList<Person>();
        int counter = 0;
        while (true) {
            persons.add(new Person());
            System.out.println("instance Count = " + (++ counter));
        }
    }
}