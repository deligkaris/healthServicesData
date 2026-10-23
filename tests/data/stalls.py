'''A background thread for run.py that, when no spark job or task has made progress for a while, prints to stderr
what the driver is doing: the python stack of the tests, the spark jobs, stages and executors, the driver heap and gc,
and the stacks of the driver JVM threads, taken a few times a minute apart.'''
import re
import sys
import threading
import time
import traceback
from datetime import datetime
from pathlib import Path

from pyspark import SparkContext

repo = Path(__file__).resolve().parents[2]
#below the top of a deep stack, the frames that name the optimizer rule, the action or the writer
keyFrame = re.compile(r"catalyst\.(optimizer|analysis)|execution\.adaptive|QueryExecution\.|DAGScheduler\.runJob|"
                      r"sql\.Dataset\.|FileFormatWriter|Committer")
topFrames = 12
maxFrames = 30

def start(out, stallMinutes, dumps, dumpSeconds, pollSeconds=60):
    '''Starts the thread; out is a stream pytest does not capture.'''
    mainThreadId = threading.main_thread().ident
    threading.Thread(target=watch, args=(out, mainThreadId, stallMinutes * 60, dumps, dumpSeconds, pollSeconds),
                     name="stalls", daemon=True).start()

def watch(out, mainThreadId, stallSeconds, dumps, dumpSeconds, pollSeconds):
    (last, since, reported) = (None, time.time(), False)
    while True:
        time.sleep(pollSeconds)
        #the spark fixture of tests/data/conftest.py starts the context
        sc = SparkContext._active_spark_context
        #the context is set before its JVM side while it starts
        if getattr(sc, "_jsc", None) is None:
            continue
        try:
            progress = get_progress(sc)
            if progress != last:
                (last, since, reported) = (progress, time.time(), False)
            elif not reported and time.time() - since >= stallSeconds:
                reported = True
                report(sc, out, mainThreadId, time.time() - since, dumps, dumpSeconds)
        except Exception as e:
            #never stop or fail the tests, the context may also be stopping
            print(f"stalls: {type(e).__name__}: {e}", file=out, flush=True)

def get_progress(sc):
    '''The newest job and the tasks finished by each active stage, which change whenever spark makes progress.'''
    tracker = sc.statusTracker()
    jobs = tracker.getJobIdsForGroup() + tracker.getActiveJobsIds()
    stages = [tracker.getStageInfo(i) for i in tracker.getActiveStageIds()]
    return max(jobs, default=-1), tuple(sorted((s.stageId, s.numCompletedTasks) for s in stages if s is not None))

def report(sc, out, mainThreadId, stalledSeconds, dumps, dumpSeconds):
    def write(line=""):
        print(line, file=out, flush=True)
    #written before any JVM call: if nothing follows, the driver JVM itself is not answering (eg stuck in gc)
    write(f"\n===== stalls: no spark progress for {stalledSeconds / 60:.0f} min, {datetime.now():%Y-%m-%d %H:%M:%S}")
    write("python (the tests' frames):")
    for line in get_python_stack(mainThreadId):
        write("  " + line)
    previous = None
    for dump in range(dumps):
        if dump:
            time.sleep(dumpSeconds)
        state = get_driver_state(sc)
        write(f"-- dump {dump + 1}/{dumps}, {datetime.now():%H:%M:%S}: {format_status(sc, state, previous)}")
        for threadId, (name, threadState, cpu, frames) in state["threads"].items():
            if previous is None or threadId not in previous["threads"]:
                write(f"  {name} {threadState} cpu {cpu:.0f} s")
                for frame in select_frames(frames):
                    write("    " + frame)
            else:
                #the later dumps show whether the thread moved and how much cpu it used meanwhile
                used = cpu - previous["threads"][threadId][2]
                write(f"  {name} {threadState} cpu +{used:.0f} s, top {frames[0] if frames else '-'}")
        previous = state
    write("=====")

def get_python_stack(mainThreadId):
    frame = sys._current_frames().get(mainThreadId)
    if frame is None:
        return ["(main thread gone)"]
    stack = traceback.extract_stack(frame)
    #the repo's frames say which test and line, the innermost four which pyspark call; pytest's own are left out
    keep = [f for i, f in enumerate(stack) if str(repo) in f.filename or i >= len(stack) - 4]
    return [f"{f.filename.replace(str(repo) + '/', '')}:{f.lineno} {f.name}: {f.line}" for f in keep]

def get_driver_state(sc):
    '''The driver JVM threads running a python call or spark sql code, with their state, cpu seconds and frames, and
    the driver's executors, heap and total gc seconds.'''
    jvm = sc._jvm
    management = jvm.java.lang.management.ManagementFactory
    mx = management.getThreadMXBean()
    traces = jvm.java.lang.Thread.getAllStackTraces()
    threads = {}
    for thread in traces.keySet():
        #one string per thread rather than one py4j call per frame; stringOf takes the array as an Object, which py4j
        #can pass, unlike the Object[] of Arrays.toString
        text = jvm.scala.runtime.ScalaRunTime.stringOf(traces[thread])[len("Array("):-1]
        frames = text.split(", ") if text else []
        threadState = thread.getState().toString()
        #py4j.commands: a thread running a call of the tests; this thread's own call is left out
        running = any("py4j.commands" in f for f in frames) and not any("getAllStackTraces" in f for f in frames[:3])
        if running or (threadState == "RUNNABLE" and any("org.apache.spark.sql" in f for f in frames)):
            threadId = thread.getId()
            threads[threadId] = (thread.getName(), threadState, mx.getThreadCpuTime(threadId) / 1e9, frames)
    runtime = jvm.java.lang.Runtime.getRuntime()
    return {"threads": threads,
            #getExecutorMemoryStatus lists the driver too
            "executors": sc._jsc.sc().getExecutorMemoryStatus().size() - 1,
            "heapGb": (runtime.totalMemory() - runtime.freeMemory()) / 2**30, "maxHeapGb": runtime.maxMemory() / 2**30,
            "gcSeconds": sum(b.getCollectionTime() for b in management.getGarbageCollectorMXBeans()) / 1000}

def format_status(sc, state, previous):
    tracker = sc.statusTracker()
    stages = [tracker.getStageInfo(i) for i in tracker.getActiveStageIds()]
    stages = ", ".join(f"{s.stageId}: {s.numActiveTasks} running {s.numCompletedTasks}/{s.numTasks} done"
                       for s in stages if s is not None) or "none"
    gc = (f"gc +{state['gcSeconds'] - previous['gcSeconds']:.0f} s" if previous is not None
          else f"gc {state['gcSeconds']:.0f} s total")
    return (f"jobs {tracker.getActiveJobsIds() or 'none'}, stages {stages}, executors {state['executors']}, "
            f"heap {state['heapGb']:.1f}/{state['maxHeapGb']:.1f} GB, {gc}")

def select_frames(frames):
    '''The top of the stack and the key frames below it, each once.'''
    selected = list(dict.fromkeys(frames[:topFrames] + [f for f in frames[topFrames:] if keyFrame.search(f)]))
    return selected[:maxFrames] + ([f"... {len(selected) - maxFrames} more"] if len(selected) > maxFrames else [])
