#!/usr/bin/env groovy

/*
 * Copyright (c) 2010, The Regents of the University of California, through Lawrence Berkeley
 * National Laboratory (subject to receipt of any required approvals from the U.S. Dept. of Energy).
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided
 * that the following conditions are met:
 *
 * (1) Redistributions of source code must retain the above copyright notice, this list of conditions and the
 * following disclaimer.
 *
 * (2) Redistributions in binary form must reproduce the above copyright notice, this list of conditions
 * and the following disclaimer in the documentation and/or other materials provided with the distribution.
 *
 * (3) Neither the name of the University of California, Lawrence Berkeley National Laboratory, U.S. Dept.
 * of Energy, nor the names of its contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * You are under no obligation whatsoever to provide any bug fixes, patches, or upgrades to the
 * features, functionality or performance of the source code ("Enhancements") to anyone; however,
 * if you choose to make your Enhancements available either publicly, or directly to Lawrence Berkeley
 * National Laboratory, without imposing a separate written license agreement for such Enhancements,
 * then you hereby grant the following license: a  non-exclusive, royalty-free perpetual license to install,
 * use, modify, prepare derivative works, incorporate into other computer software, distribute, and
 * sublicense such enhancements or derivative works thereof, in binary and source code form.
 */

/* --------------------------------------------------------------------------
   job_history.cgi

   this groovlet (groovy servlet) takes as input one of two possible parameters
   and loads the a log file associated with a hadoop job.  It parses the log
   file and generates a time graph that shows the number of instances of
   different hadoop processes (map, shuffle, reduce) on the Y-axis versus time
   units on the X-Axis.  Stacked graph seems to show nicely what's going on
   in the run.

   url parameters can be either query or post, either way.
      url=<url to log file>
      log=<full contents of logfile>

   --------------------------------------------------------------------------*/

import org.apache.log4j.Logger
import org.apache.log4j.PropertyConfigurator
import com.oreilly.servlet.multipart.MultipartParser
import java.util.regex.Matcher
import org.jfree.data.category.CategoryDataset
import org.jfree.data.general.DatasetUtilities
import org.jfree.chart.JFreeChart
import org.jfree.chart.plot.CategoryPlot
import org.jfree.chart.plot.PlotOrientation
import java.awt.Color
import org.jfree.chart.ChartRenderingInfo
import org.jfree.chart.entity.StandardEntityCollection
import org.jfree.chart.ChartUtilities
import org.jfree.chart.ChartFactory

props = new Properties().load(Logger.getClassLoader().getResourceAsStream("hadoop-jobanalyzer.properties"))
PropertyConfigurator.configure(props)
Logger log = Logger.getLogger("hadooop-jobanalyzer");

/*
* parameters are sent via post, but some existing gridauth clients use multi-part
* form data for all their data values.  this is due in part to how libcurl works with
* php, and standard servelets don't support multi-part forms.  but for the sake of
* backward compatibility, this script supports both wwww-urlencoded and multi-part
*/
post = [:]

try {
  def multi = new MultipartParser(request, 1024 * 100)
  while ((part = multi.readNextPart()) != null) {
    if (part.isParam()) {
      post[part.getName()] = part.getStringValue()
      log.debug("post = ${post}")
    }
  }
} catch (Exception e) {
  // don't do anything
}
request.getParameterMap().each {k, v ->  post[k] = v[0] }

response.setContentType("text/html");

println(""""<body>
            <IMG SRC="barchart.png" WIDTH="1200" HEIGHT="600" BORDER="0" USEMAP="#chart">
            </body>""");

def input = null;
if (post.url) {
  input = new URL(post.url);
} else if (post.log) {
  input = post.log
}

pat = /([^=]+)="([^"]*)" */
groupPat = /\{\(([^)]+)\)\(([^)]+)\)([^}]+)\}/
counterPat = /\[\(([^)]+)\)\(([^)]+)\)\(([^)]+)\)\]/
String remainder = "";
String seperator = ", "

long scale = 1000;
def job = [:]
def mapTask = [:]
def reduceTask = [:]
def mapStartTime = [:]
def mapEndTime = [:]
def finalAttempt = [:]
def wastedAttempts = []
def reduceStartTime = [:]
def reduceEndTime = [:]
def reduceShuffleTime = [:]
def reduceSortTime = [:]
def reduceBytes = [:]
long submitTime, finishTime;

input.eachLine() {line ->

  if (line.length() < 3 || !line.endsWith(" .")) {
    remainder += line;
  } else {
    line = remainder + line;
    remainder = ""

    String[] words = line.split(" ", 2);
    String event = words[0];

    Matcher m = (words[1] =~ pat);
    Map attrs = [:];

    for (int i = 0; i < m.size(); i++) {
      match = m[i];
      attrs[match[1]] = match[2];
    }
    if (event == 'Job') {
      attrs.each {k, v ->
        job[k] = v
      }
      if (attrs.containsKey("SUBMIT_TIME")) {
        submitTime = (attrs["SUBMIT_TIME"].toLong()) / scale;
      } else if (attrs.containsKey("FINISH_TIME")) {
        finishTime = (attrs["FINISH_TIME"].toLong()) / scale;
      }
    } else if (event == 'MapAttempt') {
      if (attrs["TASK_TYPE"] == "CLEANUP" || attrs["TASK_TYPE"] == "SETUP") {
        return
      }
      if (mapTask[attrs["TASKID"]] == null) {
        mapTask[attrs["TASKID"]] = [:]
        mapTask[attrs["TASKID"]]["NUM_ATTEMPTS"] = 0;
      }
      if (mapTask[attrs["TASKID"]][attrs["TASK_ATTEMPT_ID"]] == null) {
        mapTask[attrs["TASKID"]][attrs["TASK_ATTEMPT_ID"]] = [:]
        mapTask[attrs["TASKID"]]["NUM_ATTEMPTS"] = mapTask[attrs["TASKID"]]["NUM_ATTEMPTS"] + 1;
      }
      attrs.each {k, v ->
        mapTask[attrs["TASKID"]][attrs["TASK_ATTEMPT_ID"]][k] = v;
      }
      if (attrs.containsKey("START_TIME")) {
        long time = attrs["START_TIME"].toLong() / scale;
        if (time != 0) {
          mapStartTime[attrs["TASK_ATTEMPT_ID"]] = time

        }
      } else if (attrs.containsKey("FINISH_TIME")) {
        long time = attrs["FINISH_TIME"].toLong() / scale;
        mapEndTime[attrs["TASK_ATTEMPT_ID"]] = time
        if (attrs["TASK_STATUS"] == "SUCCESS") {
          task = attrs["TASKID"]
          if (finalAttempt.containsKey(task)) {
            wastedAttempts.add(finalAttempt[task])
          }
          finalAttempt[task] = attrs["TASK_ATTEMPT_ID"]
        } else {
          wastedAttempts.add(attrs["TASK_ATTEMPT_ID"])
        }
      }
    } else if (event == 'ReduceAttempt') {
      if (reduceTask[attrs["TASKID"]] == null) {
        reduceTask[attrs["TASKID"]] = [:]
        reduceTask[attrs["TASKID"]]["NUM_ATTEMPTS"] = 0;
      }
      if (reduceTask[attrs["TASKID"]][attrs["TASK_ATTEMPT_ID"]] == null) {
        reduceTask[attrs["TASKID"]][attrs["TASK_ATTEMPT_ID"]] = [:]
        reduceTask[attrs["TASKID"]]["NUM_ATTEMPTS"] = reduceTask[attrs["TASKID"]]["NUM_ATTEMPTS"] + 1;
      }
      attrs.each {k, v ->
        reduceTask[attrs["TASKID"]][attrs["TASK_ATTEMPT_ID"]][k] = v;
      }
      if (attrs.containsKey("START_TIME")) {
        long time = (long) attrs["START_TIME"].toLong() / scale
        if (time != 0)
          reduceStartTime[attrs["TASK_ATTEMPT_ID"]] = time
      } else if (attrs.containsKey("FINISH_TIME")) {
        task = attrs["TASKID"]
        if (attrs["TASK_STATUS"] == "SUCCESS") {
          if (finalAttempt.containsKey(task))
            wastedAttempts.add(finalAttempt[task])
          finalAttempt[task] = attrs["TASK_ATTEMPT_ID"]
        } else {
          wastedAttempts.add(attrs["TASK_ATTEMPT_ID"])
        }
        reduceEndTime[attrs["TASK_ATTEMPT_ID"]] = (long) ((attrs["FINISH_TIME"]).toLong() / scale)
        if (attrs.containsKey("SHUFFLE_FINISHED"))
          reduceShuffleTime[attrs["TASK_ATTEMPT_ID"]] = (long) ((attrs["SHUFFLE_FINISHED"]).toLong() / scale)
        if (attrs.containsKey("SORT_FINISHED"))
          reduceSortTime[attrs["TASK_ATTEMPT_ID"]] = (long) ((attrs["SORT_FINISHED"]).toLong() / scale)
      }
    } else if (event == 'Task') {
      if (attrs["TASK_TYPE"] == "MAP") {
        if (mapTask[attrs["TASKID"]] == null) {
          mapTask[attrs["TASKID"]] = [:]
          mapTask[attrs["TASKID"]]["NUM_ATTEMPTS"] = 0;
        }
        attrs.each {k, v ->
          mapTask[attrs["TASKID"]][k] = v;
        }
      }
      if (attrs["TASK_TYPE"] == "REDUCE") {
        if (reduceTask[attrs["TASKID"]] == null) {
          reduceTask[attrs["TASKID"]] = [:]
          reduceTask[attrs["TASKID"]]["NUM_ATTEMPTS"] = 0;
        }
        attrs.each {k, v ->
          reduceTask[attrs["TASKID"]][k] = v;
        }
      }
      if (attrs["TASK_TYPE"] == "REDUCE" && attrs.containsKey("COUNTERS")) {
        def mm = attrs["COUNTERS"] =~ groupPat;

        def counters = [:]
        for (int i = 0; i < mm.size(); i++) {
          match = mm[i];
          def c = [:]
          def mmx = match[3] =~ counterPat
          for (int ii = 0; ii < mmx.size(); ii++) {
            mmatch = mmx[ii];
            c[mmatch[2]] = mmatch[3].toLong();
          }
          counters[match[2]] = c;
        }
        reduceBytes[attrs["TASKID"]] = counters['FileSystemCounters']['HDFS_BYTES_WRITTEN'];
      }
    }
  }
}

long numMaps = 0, totalMapTime = 0;
long allMapsComplete = 0;
mapTask.keySet().each {taskid ->
  task = mapTask.get(taskid);
  if (task["FINISH_TIME"] != null) {
    totalMapTime += (long) (task["FINISH_TIME"].toLong() / scale - task["START_TIME"].toLong() / scale)
    numMaps++;
    allMapsComplete = Math.max(allMapsComplete, task["FINISH_TIME"].toLong() / scale)
  }
}

long numReduce = 0, totalReduceTime = 0, totalShuffleTime = 0;
reduceTask.keySet().each {taskid ->
  task = reduceTask.get(taskid);
  if (task["FINISH_TIME"] != null) {
    // finish_time is null if reduce job failed.
    totalReduceTime += (long) (task["FINISH_TIME"].toLong() / scale - task["START_TIME"].toLong() / scale)
    totalShuffleTime += (long) (reduceShuffleTime[finalAttempt[taskid]] - task["START_TIME"].toLong() / scale)
    numReduce++;
  }
}

def runningMaps = [:]
def shufflingReduces = [:]
def sortingReduces = [:]
def runningReduces = [:]
def waste = [:]
def finals = [:]

finalAttempt.values().each {t ->
  finals[t] = "none";
}

for (long t = 0; t < (finishTime - submitTime) + 1; t++) {
  runningMaps[t] = 0
  shufflingReduces[t] = 0
  sortingReduces[t] = 0
  runningReduces[t] = 0
  waste[t] = 0
}

mapEndTime.keySet().each {map ->
  isFinal = finals.containsKey(map)
  if (mapStartTime.containsKey(map)) {
    for (long t = (mapStartTime[map] - submitTime); t <= (Math.min(mapEndTime[map], finishTime) - submitTime); t++) {
      if (isFinal && runningMaps[t] != null) {
        runningMaps[t] += 1
      } else {
        if (waste[t] == null) {
          println("waste[t] is null t = " + t + " keyset = " + waste.get(t))
          println("map = " + map)
          println("mapstartTime - submitTime = " + (mapStartTime[map] - submitTime))
          println("mapendTime - submitTime = " + (Math.min(mapEndTime[map], finishTime) - submitTime))
        }
        waste[t] += 1
      }
    }
  }
}

for (reduce in reduceEndTime.keySet()) {
  if (reduceStartTime.containsKey(reduce)) {
    if (finals.containsKey(reduce)) {
      for (long t = (reduceStartTime[reduce] - submitTime); t <= (Math.min(reduceShuffleTime[reduce], finishTime) - submitTime); t++) {
        if (shufflingReduces[t] == null) {
          println("shufflingreduces[t] is null t = " + t)
          println("reduce = " + reduce)
        }
        shufflingReduces[t] += 1
      }
      for (long t = (reduceShuffleTime[reduce] - submitTime); t <= (Math.min(reduceSortTime[reduce], finishTime) - submitTime); t++) {
        sortingReduces[t] += 1
      }
      for (long t = (reduceSortTime[reduce] - submitTime); t <= (Math.min(reduceEndTime[reduce], finishTime) - submitTime); t++) {
        runningReduces[t] += 1
      }
    } else {
      for (long t = (reduceStartTime[reduce] - submitTime); t <= (Math.min(reduceEndTime[reduce], finishTime) - submitTime); t++) {
        waste[t] += 1
      }
    }
  }
}


final double[][] data = [runningMaps.values(), shufflingReduces.values(), sortingReduces.values(), runningReduces.values(), waste.values()];
final CategoryDataset dataset = DatasetUtilities.createCategoryDataset(
        "Maps", "", data);

JFreeChart chart = ChartFactory.createStackedBarChart(input.toString(),
        "time", // domain axis label
        "number of instances", // range axis label
        dataset, // data
        PlotOrientation.VERTICAL,
        false, // include legend
        true, // tooltips?
        false // URLs?
);

chart.setBackgroundPaint(new Color(249, 231, 236));

CategoryPlot plot = chart.getCategoryPlot();
plot.getRenderer().setSeriesPaint(0, new Color(128, 0, 0));
plot.getRenderer().setSeriesPaint(1, new Color(0, 0, 255));
plot.getRenderer().setSeriesPaint(2, new Color(0, 255, 255));
plot.getRenderer().setSeriesPaint(3, new Color(255, 255, 0));
plot.getRenderer().setSeriesPaint(4, new Color(255, 255, 255));

try {
  final ChartRenderingInfo info = new ChartRenderingInfo
  (new StandardEntityCollection());
  final File file1 = new File(application.getRealPath("/WEB-INF") + "/../barchart.png");
  ChartUtilities.saveChartAsPNG(file1, chart, 600, 400, info);
} catch (Exception e) {
  out.println(e);
}



