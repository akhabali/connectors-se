/*
 * Copyright (C) 2006-2018 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package org.talend.components.salesforce.service;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import com.sforce.soap.partner.Error;

/**
 * Contains only runtime helper classes, mainly to do with logging.
 */
public class SalesforceRuntimeHelper {

    private SalesforceRuntimeHelper() {
    }

    public static StringBuilder addLog(Error[] resultErrors, String row_key, BufferedWriter logWriter) {
        StringBuilder errors = new StringBuilder("");
        if (resultErrors != null) {
            for (Error error : resultErrors) {
                errors.append(error.getMessage()).append("\n");
                if (logWriter != null) {
                    try {
                        logWriter.append("\tStatus Code: ").append(error.getStatusCode().toString());
                        logWriter.newLine();
                        logWriter.newLine();
                        logWriter.append("\tRowKey/RowNo: " + row_key);
                        if (error.getFields() != null) {
                            logWriter.newLine();
                            logWriter.append("\tFields: ");
                            boolean flag = false;
                            for (String field : error.getFields()) {
                                if (flag) {
                                    logWriter.append(", ");
                                } else {
                                    flag = true;
                                }
                                logWriter.append(field);
                            }
                        }
                        logWriter.newLine();
                        logWriter.newLine();

                        logWriter.append("\tMessage: ").append(error.getMessage());

                        logWriter.newLine();

                        logWriter
                                .append("\t--------------------------------------------------------------------------------");

                        logWriter.newLine();
                        logWriter.newLine();
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
        return errors;
    }

    /**
     * Convert date to calendar with timezone "GMT"
     * 
     * @param date
     * @param useLocalTZ whether use local timezone during convert
     *
     * @return Calendar instance
     */
    public static Calendar convertDateToCalendar(Date date, boolean useLocalTZ) {
        if (date != null) {
            Calendar cal = Calendar.getInstance();
            cal.clear();
            if (useLocalTZ) {
                TimeZone tz = TimeZone.getDefault();
                cal.setTimeInMillis(date.getTime() + tz.getRawOffset() + tz.getDSTSavings());
            } else {
                cal.setTimeZone(TimeZone.getTimeZone("GMT"));
                cal.setTime(date);
            }
            return cal;
        } else {
            return null;
        }
    }

}
