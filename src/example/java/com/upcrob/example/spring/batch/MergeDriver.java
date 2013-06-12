package com.upcrob.example.spring.batch;

import org.springframework.batch.core.launch.support.CommandLineJobRunner;

public class MergeDriver {
    public static void main(String[] args) {
        try {
            CommandLineJobRunner.main(new String[]{"./jobConfig.xml", "simpleMergeJob"});
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
