'''A background thread for run.py that, when no spark job or task has made progress for a while, prints to stderr
what the driver is doing: the python stack of the tests, the spark jobs, stages and executors, the driver heap and gc,
and the stacks of the driver JVM threads, taken a few times a minute apart. A driver JVM that does not answer is
reported as such, with the python stack alone.'''
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
noAnswer = object()

def start(out, stallMinutes, dumps, dumpSeconds, pollSeconds=60, jvmSeconds=120):
    '''Starts the thread; out is a stream pytest does not capture.'''
    mainThreadId = threading.main_thread().ident
    threading.Thread(target=watch, args=(out, mainThreadId, stallMinutes * 60, dumps, dumpSeconds, pollSeconds,
                                         JvmCalls(jvmSeconds)),
                     name="stalls", daemon=True).start()

class JvmCalls:
    '''Runs the JVM calls one at a time on a thread of their own, and returns noAnswer after seconds without a reply.'''
    def __init__(self, seconds):
        self.seconds = seconds
        self.pending = None

    def run(self, call):
        #a JVM that stops answering (eg its heap full, in back to back gc) blocks a py4j call for good, so the watcher
        #waits a bounded time; no other call starts while one still waits, which keeps it to one stuck thread
        if self.pending is not None and self.pending.is_alive():
            return noAnswer
        result = {}
        def target():
            try:
                result["value"] = call()
            except Exception as e:
                result["error"] = e
        self.pending = threading.Thread(target=target, name="stalls-jvm", daemon=True)
        self.pending.start()
        self.pending.join(self.seconds)
        if self.pending.is_alive():
            return noAnswer
        if "error" in result:
            raise result["error"]
        return result["value"]

def watch(out, mainThreadId, stallSeconds, dumps, dumpSeconds, pollSeconds, jvm):
    (last, since, reported) = (None, time.time(), False)
    while True:
        time.sleep(pollSeconds)
        #the spark fixture of tests/data/conftest.py starts the context
        sc = SparkContext._active_spark_context
        #the context is set before its JVM side while it starts
        if getattr(sc, "_jsc", None) is None:
            continue
        try:
            progress = jvm.run(lambda: get_progress(sc))
            #no answer counts as no progress, nothing moves while the JVM does not answer
            if progress is not noAnswer and progress != last:
                (last, since, reported) = (progress, time.time(), False)
            elif not reported and time.time() - since >= stallSeconds:
                reported = True
                report(sc, out, mainThreadId, jvm, time.time() - since, dumps, dumpSeconds)
        except Exception as e:
            #never stop or fail the tests, the context may also be stopping
            print(f"stalls: {type(e).__name__}: {e}", file=out, flush=True)

def get_progress(sc):
    '''The newest job and the tasks finished by each active stage, which change whenever spark makes progress.'''
    tracker = sc.statusTracker()
    jobs = tracker.getJobIdsForGroup() + tracker.getActiveJobsIds()
    stages = [tracker.getStageInfo(i) for i in tracker.getActiveStageIds()]
    return max(jobs, default=-1), tuple(sorted((s.stageId, s.numCompletedTasks) for s in stages if s is not None))

def report(sc, out, mainThreadId, jvm, stalledSeconds, dumps, dumpSeconds):
    def write(line=""):
        print(line, file=out, flush=True)
    #the header and the python stack need no JVM call, so they print even when the JVM does not answer
    write(f"\n===== stalls: no spark progress for {stalledSeconds / 60:.0f} min, {datetime.now():%Y-%m-%d %H:%M:%S}")
    write("python (the tests' frames):")
    for line in get_python_stack(mainThreadId):
        write("  " + line)
    previous = None
    for dump in range(dumps):
        if dump:
            time.sleep(dumpSeconds)
        state = jvm.run(lambda: get_driver_state(sc))
        if state is noAnswer:
            #a call still waiting has waited at least jvm.seconds
            write(f"-- dump {dump + 1}/{dumps}, {datetime.now():%H:%M:%S}: the driver JVM is not answering, a call has "
                  f"waited over {jvm.seconds} s (eg its heap full, in back to back gc)")
            break
        write(f"-- dump {dump + 1}/{dumps}, {datetime.now():%H:%M:%S}: {format_status(state, previous)}")
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
    the driver's active jobs and stages, executors, heap and total gc seconds.'''
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
    tracker = sc.statusTracker()
    stages = [tracker.getStageInfo(i) for i in tracker.getActiveStageIds()]
    return {"threads": threads, "jobs": tracker.getActiveJobsIds(), "stages": [s for s in stages if s is not None],
            #getExecutorMemoryStatus lists the driver too
            "executors": sc._jsc.sc().getExecutorMemoryStatus().size() - 1,
            "heapGb": (runtime.totalMemory() - runtime.freeMemory()) / 2**30, "maxHeapGb": runtime.maxMemory() / 2**30,
            "gcSeconds": sum(b.getCollectionTime() for b in management.getGarbageCollectorMXBeans()) / 1000}

def format_status(state, previous):
    stages = ", ".join(f"{s.stageId}: {s.numActiveTasks} running {s.numCompletedTasks}/{s.numTasks} done"
                       for s in state["stages"]) or "none"
    gc = (f"gc +{state['gcSeconds'] - previous['gcSeconds']:.0f} s" if previous is not None
          else f"gc {state['gcSeconds']:.0f} s total")
    return (f"jobs {state['jobs'] or 'none'}, stages {stages}, executors {state['executors']}, "
            f"heap {state['heapGb']:.1f}/{state['maxHeapGb']:.1f} GB, {gc}")

def select_frames(frames):
    '''The top of the stack and the key frames below it, each once.'''
    selected = list(dict.fromkeys(frames[:topFrames] + [f for f in frames[topFrames:] if keyFrame.search(f)]))
    return selected[:maxFrames] + ([f"... {len(selected) - maxFrames} more"] if len(selected) > maxFrames else [])
