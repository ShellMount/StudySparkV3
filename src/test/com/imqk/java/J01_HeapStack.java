package com.imqk.java;


import java.util.ArrayList;
import java.util.List;

// 堆栈益出模拟
// JVM 运行参数：
// -verbose:gc -Xms10M -Xmx10M -XX:MaxDirectMemorySize=5M -Xss128k -XX:+PrintGCDetails

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