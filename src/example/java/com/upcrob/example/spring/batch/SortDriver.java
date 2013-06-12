package com.upcrob.example.spring.batch;

import org.springframework.batch.core.launch.support.CommandLineJobRunner;

/**
 * Simple driver that demonstrates the use of the FlatFileSortTasklet.
 */
public class SortDriver {
    public static void main(String[] args) {
        try {
            CommandLineJobRunner.main(new String[]{"./jobConfig.xml", "simpleSortJob"});
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
