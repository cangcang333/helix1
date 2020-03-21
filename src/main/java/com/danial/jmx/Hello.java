package com.danial.jmx;

public class Hello implements HelloMBean{
    private String name;
    private String age;

    @Override
    public String getName() {
        System.out.println("get name");
        return name;
    }

    @Override
    public void setName(String name) {
        System.out.println("set name " + name);
        this.name = name;
    }

    @Override
    public String getAge() {
        System.out.println("get age");
        return age;
    }

    @Override
    public void setAge(String age) {
        System.out.println("set age " + age);
        this.age = age;
    }

    @Override
    public void helloWorld() {
        System.out.println("hello Shanghai");
    }

    @Override
    public void helloWorld(String str) {
        System.out.println("hello Shanghai " + str);
    }

    @Override
    public void getTelephone() {
        System.out.println("get Telephone");
    }
}
