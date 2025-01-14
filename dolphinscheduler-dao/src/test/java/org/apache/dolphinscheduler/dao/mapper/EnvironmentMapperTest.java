/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.dao.mapper;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import org.apache.dolphinscheduler.dao.BaseDaoTest;
import org.apache.dolphinscheduler.dao.entity.Environment;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Date;
import java.util.List;

public class EnvironmentMapperTest extends BaseDaoTest {

    @Autowired
    EnvironmentMapper environmentMapper;

    /**
     * insert
     *
     * @return Environment
     */
    private Environment insertOne() {
        //insertOne
        Environment environment = new Environment();
        environment.setName("testEnv");
        environment.setCode(1L);
        environment.setOperator(1);
        environment.setConfig(getConfig());
        environment.setDescription(getDesc());
        environment.setCreateTime(new Date());
        environment.setUpdateTime(new Date());
        environmentMapper.insert(environment);
        return environment;
    }

    @Before
    public void setUp() {
        clearTestData();
    }

    @After
    public void after() {
        clearTestData();
    }

    public void clearTestData() {
        environmentMapper.queryAllEnvironmentList().stream().forEach(environment -> {
            environmentMapper.deleteByCode(environment.getCode());
        });
    }

    /**
     * test update
     */
    @Test
    public void testUpdate() {
        //insertOne
        Environment environment = insertOne();
        environment.setDescription("new description info");
        //update
        int update = environmentMapper.updateById(environment);
        Assert.assertEquals(update, 1);
    }

    /**
     * test delete
     */
    @Test
    public void testDelete() {
        Environment environment = insertOne();
        int delete = environmentMapper.deleteById(environment.getId());
        Assert.assertEquals(delete, 1);
    }

    /**
     * test query
     */
    @Test
    public void testQuery() {
        insertOne();
        //query
        List<Environment> environments = environmentMapper.selectList(null);
        Assert.assertEquals(environments.size(), 1);
    }

    /**
     * test query environment by name
     */
    @Test
    public void testQueryByEnvironmentName() {
        Environment entity = insertOne();
        Environment environment = environmentMapper.queryByEnvironmentName(entity.getName());
        Assert.assertEquals(entity.toString(),environment.toString());
    }

    /**
     * test query environment by code
     */
    @Test
    public void testQueryByEnvironmentCode() {
        Environment entity = insertOne();
        Environment environment = environmentMapper.queryByEnvironmentCode(entity.getCode());
        Assert.assertEquals(entity.toString(),environment.toString());
    }

    /**
     * test query all environments
     */
    @Test
    public void testQueryAllEnvironmentList() {
        Environment entity = insertOne();
        List<Environment> environments = environmentMapper.queryAllEnvironmentList();
        Assert.assertEquals(environments.size(), 1);
        Assert.assertEquals(entity.toString(),environments.get(0).toString());
    }

    /**
     * test query environment list paging
     */
    @Test
    public void testQueryEnvironmentListPaging() {
        Environment entity = insertOne();
        Page<Environment> page = new Page<>(1, 10);
        IPage<Environment> environmentIPage = environmentMapper.queryEnvironmentListPaging(page,"");
        List<Environment> environmentList = environmentIPage.getRecords();
        Assert.assertEquals(environmentList.size(), 1);

        environmentIPage = environmentMapper.queryEnvironmentListPaging(page,"abc");
        environmentList = environmentIPage.getRecords();
        Assert.assertEquals(environmentList.size(), 0);
    }

    /**
     * test query all environments
     */
    @Test
    public void testDeleteByCode() {
        Environment entity = insertOne();
        int delete = environmentMapper.deleteByCode(entity.getCode());
        Assert.assertEquals(delete, 1);
    }

    private String getDesc() {
        return "create an environment to test ";
    }

    /**
     * create an environment config
     */
    private String getConfig() {
        return "export HADOOP_HOME=/opt/hadoop-2.6.5\n"
                + "export HADOOP_CONF_DIR=/etc/hadoop/conf\n"
                + "export SPARK_HOME1=/opt/soft/spark1\n"
                + "export SPARK_HOME2=/opt/soft/spark2\n"
                + "export PYTHON_HOME=/opt/soft/python\n"
                + "export JAVA_HOME=/opt/java/jdk1.8.0_181-amd64\n"
                + "export HIVE_HOME=/opt/soft/hive\n"
                + "export FLINK_HOME=/opt/soft/flink\n"
                + "export DATAX_HOME=/opt/soft/datax\n"
                + "export YARN_CONF_DIR=\"/etc/hadoop/conf\"\n"
                + "\n"
                + "export PATH=$HADOOP_HOME/bin:$SPARK_HOME1/bin:$SPARK_HOME2/bin:$PYTHON_HOME/bin:$JAVA_HOME/bin:$HIVE_HOME/bin:$FLINK_HOME/bin:$DATAX_HOME/bin:$PATH\n"
                + "\n"
                + "export HADOOP_CLASSPATH=`hadoop classpath`\n"
                + "\n"
                + "#echo \"HADOOP_CLASSPATH=\"$HADOOP_CLASSPATH";
    }
}