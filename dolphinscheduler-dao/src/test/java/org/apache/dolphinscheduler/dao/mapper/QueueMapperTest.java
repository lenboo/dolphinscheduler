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
import org.apache.dolphinscheduler.dao.entity.Queue;

import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Collections;
import java.util.Date;
import java.util.List;

public class QueueMapperTest extends BaseDaoTest {

    @Autowired
    private QueueMapper queueMapper;

    /**
     * insert
     *
     * @return Queue
     */
    private Queue insertOne() {
        //insertOne
        Queue queue = new Queue();
        queue.setQueueName("queue");
        queue.setQueue("queue");
        queue.setCreateTime(new Date());
        queue.setUpdateTime(new Date());
        queueMapper.insert(queue);
        return queue;
    }

    /**
     * test update
     */
    @Test
    public void testUpdate() {
        //insertOne
        Queue queue = insertOne();
        queue.setCreateTime(new Date());
        //update
        int update = queueMapper.updateById(queue);
        Assert.assertEquals(1, update);
    }

    /**
     * test delete
     */
    @Test
    public void testDelete() {
        Queue queue = insertOne();
        int delete = queueMapper.deleteById(queue.getId());
        Assert.assertEquals(1, delete);
    }

    /**
     * test query
     */
    @Test
    public void testQuery() {
        Queue queue = insertOne();
        //query
        List<Queue> queues = queueMapper.selectList(null);
        Assert.assertNotEquals(queues.size(), 0);
    }

    /**
     * test page
     */
    @Test
    public void testQueryQueuePaging() {

        Queue queue = insertOne();
        Page<Queue> page = new Page(1,3);

        IPage<Queue> queueIPage= queueMapper.queryQueuePaging(page, Collections.singletonList(queue.getId()), null);
        Assert.assertNotEquals(queueIPage.getTotal(), 0);

        queueIPage= queueMapper.queryQueuePaging(page, Collections.singletonList(queue.getId()), queue.getQueueName());
        Assert.assertNotEquals(queueIPage.getTotal(), 0);
    }

    /**
     * test query all list
     */
    @Test
    public void queryAllQueueList() {
        Queue queue = insertOne();

        List<Queue> queues = queueMapper.queryAllQueueList(queue.getQueue(), null);
        Assert.assertNotEquals(queues.size(), 0);

        queues = queueMapper.queryAllQueueList(null, queue.getQueueName());
        Assert.assertNotEquals(queues.size(), 0);
    }

    @Test
    public void existQueue() {
        Assert.assertNull(queueMapper.existQueue("queue", null));
        Assert.assertNull(queueMapper.existQueue(null, "queue"));
        Queue queue = insertOne();
        Assert.assertTrue(queueMapper.existQueue(queue.getQueue(), null) == Boolean.TRUE);
        Assert.assertTrue(queueMapper.existQueue(null, queue.getQueueName()) == Boolean.TRUE);
    }
}
