package com.ityj.controller;

import java.time.LocalDate;

public class TestMain {


    public static void main(String[] args) {

        LocalDate localDate = LocalDate.now();
        System.out.println(localDate);
        LocalDate localDate1 = localDate.minusDays(7);
        System.out.println("localDate1 = " + localDate1);

    }

}
