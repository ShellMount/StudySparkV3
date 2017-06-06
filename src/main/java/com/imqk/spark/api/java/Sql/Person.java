package com.imqk.spark.api.java.Sql;

import java.io.Serializable;

/**
 * Created by 428900 on 2017/5/9.
 */

public class Person implements Serializable{

    private static final long serialVersionUID = 1L;
    private  int id;
    private  String name;
    private  int age;

    @Override
    public String toString() {
        return "Person{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", age=" + age +
                '}';
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }
}