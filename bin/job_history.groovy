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

/* ==================================== */
/* = commandline processing functions = */
/* ==================================== */


/**
 *  processes the argments from commandline and returns map.
 *  flags are boolean and params take the following argument as value
 *
 * @param flag list of flags in the form ['-b', '-d',]
 * @param params is list of flags that take a value
 * @param args is the commandline argment as list
 * @return map file of the form ['-d': true, '-x': 'value']
 */
Map processArgs(Set flags, Set params, List<String> args) {
  boolean error = false;
  Map options = [:]

  //
  // first process any java options of the form -Dxxx=yyy
  //
  newargs = [];
  options["java"] = "";
  args.each {a ->
    if (a ==~ /^-D[\S]+(=.*)+$/ ) {
      options["java"] = options["java"] + " " + a;
    } else {
      newargs.add(a);
    }
  }
  args = newargs;

  //
  // process the flags first
  //
  flags.each {f ->
    if (args.contains(f)) {
      options[f] = true
      args.remove(f)
    }
  }

  //
  // now for the parameters
  //
  params.each {p ->
    if (args.contains(p)) {

      // the value of the parameter is the next in sequence

      def v = args[args.findIndexOf { it == p} + 1]

      // make sure value is not a parameter

      if (v && v[0] != '-') {
        options[p] = v
        args.remove(v)
        args.remove(p)
      } else {
        // if no value is found set error but continue
        error = true
      }
    }
  }

  options['error'] = error

  //
  // now set the remaining unused args in options.args[0] ...
  //
  options['args'] = args

  return options
}

/**
 * prints the usage for the command
 */
void printUsage() {
  println("usage: cat <log_file> | job_history > outputfile")
}

/* ======================= */
/* = Script starts here  = */
/* ======================= */


/*
        setup the parameters and process commandline
*/

List largs = (List) args

Set flags = ['-d', '--help', '-?'] as Set;
Set params = [] as Set;

def options = processArgs(flags, params, largs)

if (options.error) {
  printUsage()
  System.exit(1)
}

if (options['-d']) {
  println "options = " + options
}

if (options['--help'] || options['-?']) {
  printUsage()
  System.exit(0)
}


pat = /([^=]+)="([^"]*)" */
groupPat = /\{\(([^)]+)\)\(([^)]+)\)([^}]+)\}/
counterPat = /\[\(([^)]+)\)\(([^)]+)\)\(([^)]+)\)\]/

remainder = "";
int scale = 1000;
def job = [:]
def mapTask = [:]
def reduceTask = [:]
def mapStartTime = [:]
def mapEndTime= [:]
def finalAttempt = [:]
def wastedAttempts = []
def reduceStartTime = [:]
def reduceEndTime = [:]
def reduceShuffleTime = [:]
def reduceSortTime = [:]
def reduceBytes = [:]
int submitTime, finishTime;

System.in.eachLine() {line -> 
	if (line.length() < 3 || !line.endsWith(" .")) {
      remainder += line;
    } else {
      line = remainder + line;
      remainder = ""

      String[] words = line.split(" ", 2);
      String event = words[0];

      def m = words[1] =~ pat;
      //println("m = " + m[0]);
      Map attrs = [:];
      m.each {match ->
        attrs[match[1]] = match[2];
      }
      //println("attrs = " + attrs);


      if (event == 'Job') {
        attrs.each {k, v ->
          job[k] = v
        }
        if (attrs.containsKey("SUBMIT_TIME")) {
          submitTime = (attrs["SUBMIT_TIME"].toBigInteger())/scale;
        } else if (attrs.containsKey("FINISH_TIME")) {
          finishTime = (attrs["FINISH_TIME"].toBigInteger())/scale;
        }
      } else if (event == 'MapAttempt') {
        if (mapTask[attrs["TASKID"]] == null) {
          mapTask[attrs["TASKID"]] = [:]
        }
        if (mapTask[attrs["TASKID"]][attrs["TASK_ATTEMPT_ID"]] == null) {
          mapTask[attrs["TASKID"]][attrs["TASK_ATTEMPT_ID"]] = [:]
        }
        attrs.each {k, v ->
          mapTask[attrs["TASKID"]][attrs["TASK_ATTEMPT_ID"]][k] = v;
        }
        if (attrs.containsKey("START_TIME")) {
          int time = attrs["START_TIME"].toBigInteger()/scale;
          if (time != 0) {
            mapStartTime[attrs["TASK_ATTEMPT_ID"]] = time
          }
        } else if (attrs.containsKey("FINISH_TIME")) {
          int time = attrs["FINISH_TIME"].toBigInteger()/scale;
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
        }
        if (reduceTask[attrs["TASKID"]][attrs["TASK_ATTEMPT_ID"]] == null) {
          reduceTask[attrs["TASKID"]][attrs["TASK_ATTEMPT_ID"]] = [:]
        }
        attrs.each {k, v ->
          reduceTask[attrs["TASKID"]][attrs["TASK_ATTEMPT_ID"]][k] = v;
        }
        if (attrs.containsKey("START_TIME")) {
          int time = (int) attrs["START_TIME"].toBigInteger() / scale
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
          reduceEndTime[attrs["TASK_ATTEMPT_ID"]] = (int) ((attrs["FINISH_TIME"]).toBigInteger() / scale)
          if (attrs.containsKey("SHUFFLE_FINISHED"))
            reduceShuffleTime[attrs["TASK_ATTEMPT_ID"]] = (int) ((attrs["SHUFFLE_FINISHED"]).toBigInteger() / scale)
          if (attrs.containsKey("SORT_FINISHED"))
            reduceSortTime[attrs["TASK_ATTEMPT_ID"]] = (int) ((attrs["SORT_FINISHED"]).toBigInteger() / scale)
        }
      } else if (event == 'Task') {
        if (attrs["TASK_TYPE"] == "MAP") {
          if (mapTask[attrs["TASKID"]] == null) {
            mapTask[attrs["TASKID"]] = [:]
          }
          attrs.each {k, v ->
            mapTask[attrs["TASKID"]][k] = v;
          }
        }
        if (attrs["TASK_TYPE"] == "REDUCE") {
          if (reduceTask[attrs["TASKID"]] == null) {
            reduceTask[attrs["TASKID"]] = [:]
          }
          attrs.each {k, v ->
            reduceTask[attrs["TASKID"]][k] = v;
          }
        }

        if (attrs["TASK_TYPE"] == "REDUCE" && attrs.containsKey("COUNTERS")) {
          //println("attempted match with: " + attrs["COUNTERS"])
          def mm = attrs["COUNTERS"] =~ groupPat;

          def counters = [:]

          mm.each {match ->
            def c = [:]
            def mmx = match[3] =~ counterPat
            mmx.each {mmatch -> c[mmatch[2]] = mmatch[3].toBigInteger();}
            counters[match[2]] = c;
          }
          reduceBytes[attrs["TASKID"]] = counters['FileSystemCounters']['HDFS_BYTES_WRITTEN'];
        }
      }
    }
}

/*
println("Job details: ")


void printNice(Map m, int ns) {
  m.each {k, v ->

    for (i in 0 .. ns) {
      print(" ");
    }
    if (v instanceof java.util.Map ) {
      print(k + ":\n")
      printNice(v, ns+4)
    } else {
      print (k + "=" + v + "\n");
    }
  }
}
printNice(job, 4);

println("maptask details for " + mapTask.size() + " map tasks")
mapTask.keySet().sort().each {taskid ->
  println(taskid + ":")
  printNice(mapTask.get(taskid), 4)
}

Set reduces = new TreeSet(reduceBytes.keySet());

println("Name, reduce-output-bytes, shuffle-finish, reduce-finish")
reduces.each {r ->
  attempt = finalAttempt[r]
  print (r + ", " + reduceBytes[r] +  ", " + reduceShuffleTime[attempt] - submitTime)
  println (", " + reduceEndTime[attempt] - submitTime)
}
println()
 */

def runningMaps = [:]
def shufflingReduces = [:]
def sortingReduces = [:]
def runningReduces = [:]
def waste = [:]
def finals = [:]

finalAttempt.values().each {t ->
  finals[t] = "none";
}

(0 .. finishTime - submitTime + 1).each  { t ->
  runningMaps[t] = 0
  shufflingReduces[t] = 0
  sortingReduces[t] = 0
  runningReduces[t] = 0
  waste[t] = 0
}

mapEndTime.keySet().each {map ->
  //println("map = " + map + " value = " + mapEndTime.get(map))
  isFinal = finals.containsKey(map)
  //println("isfinal = " + isFinal)
  if (mapStartTime.containsKey(map)) {
    //println("mapStartTime = " + mapStartTime.get(map))
    for (t in mapStartTime[map]-submitTime .. mapEndTime[map]-submitTime) {
      //println("t = " + t)
      if (isFinal && runningMaps[t] !=  null) {
        runningMaps[t] += 1
      } else  {
        waste[t] += 1
      }
    }
  }
}

for (reduce in reduceEndTime.keySet()) {
  if (reduceStartTime.containsKey(reduce)) {
    if (finals.containsKey(reduce)) {
      //println("checking reduce: " + reduce);
      //println("reduceStartTime = " + reduceStartTime[reduce]-submitTime + " reduceShuffleTime = " + reduceShuffleTime[reduce]-submitTime)
      for (t in reduceStartTime[reduce]-submitTime .. reduceShuffleTime[reduce]-submitTime + 1) {
        shufflingReduces[t] += 1
      }
      for (t in reduceShuffleTime[reduce]-submitTime .. reduceSortTime[reduce]-submitTime) {
        sortingReduces[t] += 1
      }
      for (t in reduceSortTime[reduce]-submitTime .. reduceEndTime[reduce]-submitTime) {
        runningReduces[t] += 1
      }
    } else {
      for (t in reduceStartTime[reduce]-submitTime .. reduceEndTime[reduce]-submitTime) {
        waste[t] += 1
      }
    }
  }
}
println("time, maps, shuffle, merge, reduce, waste");

for (t in 0 .. runningMaps.size()) {
  println(t + ", "  + runningMaps[t] + ", " + shufflingReduces[t] + ", " + sortingReduces[t] + ", " + runningReduces[t] + ", " + waste[t])
}
