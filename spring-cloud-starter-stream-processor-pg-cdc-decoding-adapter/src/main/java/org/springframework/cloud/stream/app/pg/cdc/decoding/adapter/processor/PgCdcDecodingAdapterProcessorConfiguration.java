/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.app.pg.cdc.decoding.adapter.processor;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.app.pg.cdc.decoding.adapter.processor.parser.ToWal2JsonParser;
import org.springframework.cloud.stream.app.pg.cdc.wal2json.Change;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.integration.annotation.ServiceActivator;

/**
 * @author Christian Tzolov
 */
@EnableBinding(Processor.class)
@EnableConfigurationProperties(PgCdcDecodingAdapterProcessorProperties.class)
public class PgCdcDecodingAdapterProcessorConfiguration {

    private ToWal2JsonParser parser = new ToWal2JsonParser();

    @ServiceActivator(inputChannel = Processor.INPUT, outputChannel = Processor.OUTPUT)
    public Change convertToWal2JsonFormat(String input) {
        return parser.parseLogLine(input);
    }

}
