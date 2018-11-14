// ============================================================================
//
// Copyright (C) 2006-2018 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components;

import java.io.Serializable;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.annotation.PostConstruct;

import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Input;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.record.Record;

@Version
@Icon(Icon.IconType.SAMPLE)
@Processor(name = "DataCollector", family = "MarketoTest")
public class DataCollector implements Serializable {

    private static Queue<Record> data = new ConcurrentLinkedQueue<>();

    public DataCollector() {
    }

    @PostConstruct
    public void init() {
    }

    @ElementListener
    public void onElement(@Input final Record record) {
        data.add(record);
    }

    public static Queue<Record> getData() {
        Queue<Record> records = new ConcurrentLinkedQueue<>(data);
        return records;
    }

    public static void reset() {
        data = new ConcurrentLinkedQueue<>();
    }
}
