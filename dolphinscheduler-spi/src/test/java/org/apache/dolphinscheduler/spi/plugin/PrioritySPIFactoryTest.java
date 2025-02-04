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

package org.apache.dolphinscheduler.spi.plugin;

import com.google.auto.service.AutoService;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class PrioritySPIFactoryTest {

    @Test
    public void loadHighPriority() {
        PrioritySPIFactory<LoadHighPriorityConflictTestSPI> factory = new PrioritySPIFactory<>(LoadHighPriorityConflictTestSPI.class);
        Map<String, LoadHighPriorityConflictTestSPI> spiMap = factory.getSPIMap();
        Assert.assertEquals(1, spiMap.get("A").getIdentify().getPriority());
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwExceptionWhenPriorityIsSame() {
        PrioritySPIFactory<ThrowExceptionConflictTestSPI> factory = new PrioritySPIFactory<>(ThrowExceptionConflictTestSPI.class);
        Map<String, ThrowExceptionConflictTestSPI> spiMap = factory.getSPIMap();
        Assert.assertEquals(0, spiMap.get("B").getIdentify().getPriority());
    }


    public interface LoadHighPriorityConflictTestSPI extends PrioritySPI {

    }

    @AutoService(LoadHighPriorityConflictTestSPI.class)
    public static class SPIA implements LoadHighPriorityConflictTestSPI {

        @Override
        public SPIIdentify getIdentify() {
            return SPIIdentify.create("A", 0);
        }
    }

    @AutoService(LoadHighPriorityConflictTestSPI.class)
    public static class SPIAA implements LoadHighPriorityConflictTestSPI {

        @Override
        public SPIIdentify getIdentify() {
            return SPIIdentify.create("A", 1);
        }
    }

    public interface ThrowExceptionConflictTestSPI extends PrioritySPI {

    }

    @AutoService(ThrowExceptionConflictTestSPI.class)
    public static class SPIB implements ThrowExceptionConflictTestSPI {

        @Override
        public SPIIdentify getIdentify() {
            return SPIIdentify.create("B", 0);
        }
    }

    @AutoService(ThrowExceptionConflictTestSPI.class)
    public static class SPIBB implements ThrowExceptionConflictTestSPI {

        @Override
        public SPIIdentify getIdentify() {
            return SPIIdentify.create("B", 0);
        }
    }


}