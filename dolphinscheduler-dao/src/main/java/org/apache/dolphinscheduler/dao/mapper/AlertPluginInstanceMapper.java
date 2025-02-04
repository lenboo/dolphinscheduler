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

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import org.apache.dolphinscheduler.dao.entity.AlertPluginInstance;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface AlertPluginInstanceMapper extends BaseMapper<AlertPluginInstance> {

    /**
     * query all alert plugin instance
     *
     * @return AlertPluginInstance list
     */
    List<AlertPluginInstance> queryAllAlertPluginInstanceList();

    /**
     * query by alert group id
     *
     * @param ids
     * @return AlertPluginInstance list
     */
    List<AlertPluginInstance> queryByIds(@Param("ids") List<Integer> ids);

    /**
     * Query alert plugin instance by given name
     * @param page                page
     * @param instanceName         Alert plugin name
     * @return alertPluginInstance Ipage
     */
    IPage<AlertPluginInstance> queryByInstanceNamePage(Page page, @Param("instanceName") String instanceName);

    /**
     *
     * @param instanceName instanceName
     * @return if exist return true else return null
     */
    Boolean existInstanceName(@Param("instanceName") String instanceName);
}
