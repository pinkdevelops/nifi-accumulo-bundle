/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.processors.accumulo;

import org.apache.nifi.accumulo.put.PutFlowFile;
import org.apache.nifi.accumulo.put.PutMutation;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

@EventDriven
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"hadoop", "Accumulo"})
@CapabilityDescription("Adds the Contents of a FlowFile to Accumulo as the value of a single cell")
public class PutAccumuloCell extends AbstractPutAccumulo {

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(ACCUMULO_CLIENT_SERVICE);
        properties.add(TABLE_NAME);
        properties.add(ROW_ID);
        properties.add(COLUMN_FAMILY);
        properties.add(COLUMN_QUALIFIER);
        properties.add(COLUMN_VISIBILITY);
        properties.add(BATCH_SIZE);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        return rels;
    }

    @Override
    protected PutFlowFile createPut(final ProcessSession session, final ProcessContext context, final FlowFile flowFile) {
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String row = context.getProperty(ROW_ID).evaluateAttributeExpressions(flowFile).getValue();
        final String columnFamily = context.getProperty(COLUMN_FAMILY).evaluateAttributeExpressions(flowFile).getValue();
        final String columnQualifier = context.getProperty(COLUMN_QUALIFIER).evaluateAttributeExpressions(flowFile).getValue();
        final String columnVisibility = context.getProperty(COLUMN_VISIBILITY).evaluateAttributeExpressions(flowFile).getValue();

        final byte[] buffer = new byte[(int) flowFile.getSize()];
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                StreamUtils.fillBuffer(in, buffer);
            }
        });

        final Collection<PutMutation> mutations = Collections.singletonList(new PutMutation(columnFamily, columnQualifier, columnVisibility, buffer.toString()));
        return new PutFlowFile(tableName, row, mutations, flowFile);
    }
}
