package com.mikemunhall.hbasedaotest.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;

@Configuration
@ImportResource("classpath:/hbase/hbase.xml")
public class HbaseConfig{

}
