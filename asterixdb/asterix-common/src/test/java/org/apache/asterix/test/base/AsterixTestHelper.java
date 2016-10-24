/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.test.base;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;

public class AsterixTestHelper {

    public static String extToResExt(String fname, String resultExt) {
        int dot = fname.lastIndexOf('.');
        return fname.substring(0, dot + 1) + resultExt;
    }

    public static ArrayList<String> readFile(String fileName, String basePath) {
        ArrayList<String> list = new ArrayList<String>();
        BufferedReader result;
        try {
            result = new BufferedReader(new FileReader(basePath + fileName));
            while (true) {
                String line = result.readLine();
                if (line == null) {
                    break;
                }
                if (line.length() == 0) {
                    continue;
                } else {
                    list.add(line);
                }
            }
            result.close();
        } catch (FileNotFoundException e) {
        } catch (IOException e) {
        }
        return list;
    }

    public static void readFileToString(File file, StringBuilder buf) throws Exception {
        BufferedReader result = new BufferedReader(new FileReader(file));
        while (true) {
            String s = result.readLine();
            if (s == null) {
                break;
            } else {
                buf.append(s);
                buf.append('\n');
            }
        }
        result.close();
    }

    public static void deleteRec(File path) {
        if (path.isDirectory()) {
            for (File f : path.listFiles()) {
                deleteRec(f);
            }
        }
        path.delete();
    }

    public static void deepSelectiveCopy(File srcDir, File destDir, FileFilter filter) throws IOException {
        if (!srcDir.isDirectory()) {
            throw new IllegalArgumentException("Not a directory: " + srcDir);
        }
        if (destDir.exists() && !destDir.isDirectory()) {
            throw new IllegalArgumentException("Exists and not a directory: " + destDir);
        }
        for (File child : srcDir.listFiles()) {
            if (child.isDirectory()) {
                deepSelectiveCopy(child, new File(destDir, child.getName()), filter);
            } else if (filter.accept(child)) {
                destDir.mkdirs();
                FileUtils.copyFile(child, new File(destDir, child.getName()));
            }
        }
    }

}
