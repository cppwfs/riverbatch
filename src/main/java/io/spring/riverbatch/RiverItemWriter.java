/*
 * Copyright 2017 the original author or authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.spring.riverbatch;

import java.util.List;

import org.springframework.batch.item.ItemWriter;

public class RiverItemWriter implements ItemWriter<RiverStat> {

	float maxHeight = 0.0f;
	@Override
	public void write(List<? extends RiverStat> list) throws Exception {
		for(RiverStat riverStat : list) {
			if(riverStat.getHeight() > maxHeight) {
				maxHeight = riverStat.getHeight();
			}
		}
//		throw new IllegalStateException("NO RIVER DATA FOR YOU!");
		System.out.println("MAX Height is :" + maxHeight);
	}
}
