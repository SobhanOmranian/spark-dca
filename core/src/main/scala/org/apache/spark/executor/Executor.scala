/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.executor

import java.io.{ File, NotSerializableException }
import java.lang.Thread.UncaughtExceptionHandler
import java.lang.management.ManagementFactory
import java.net.{ URI, URL }
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent._
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable.{ ArrayBuffer, HashMap, Map }
import scala.util.control.NonFatal

import com.google.common.util.concurrent.ThreadFactoryBuilder

import org.apache.spark._
import org.apache.spark.dca.SelfAdaptiveThreadPoolExecutor;
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.rpc.RpcTimeout
import org.apache.spark.scheduler.{ DirectTaskResult, IndirectTaskResult, Task, TaskDescription }
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage.{ StorageLevel, TaskResultBlockId }
import org.apache.spark.util._
import org.apache.spark.util.io.ChunkedByteBuffer
import org.apache.spark.dca.SelfAdaptiveFixedThreadPoolExecutor
import org.apache.spark.dca.SelfAdaptiveNoActionPoolExecutor
import org.apache.spark.dca.SelfAdaptiveStaticThreadPoolExecutor
import org.apache.spark.dca.MyThreadPoolExecutor

/**
 * Spark executor, backed by a threadpool to run tasks.
 *
 * This can be used with Mesos, YARN, and the standalone scheduler.
 * An internal RPC interface is used for communication with the driver,
 * except in the case of Mesos fine-grained mode.
 */
class Executor(
  executorId:               String,
  executorHostname:         String,
  env:                      SparkEnv,
  userClassPath:            Seq[URL]                 = Nil,
  isLocal:                  Boolean                  = false,
  cores:                    Int,
  changedCores:             Int,
  appId:                    String,
  executorBackEnd:          ExecutorBackend,
  uncaughtExceptionHandler: UncaughtExceptionHandler = SparkUncaughtExceptionHandler)
  extends Logging {

  logInfo(s"Starting executor ID $executorId on host $executorHostname with $cores threads")

  // Application dependencies (added through SparkContext) that we've fetched so far on this node.
  // Each map holds the master's timestamp for the version of that file or JAR we got.
  private val currentFiles: HashMap[String, Long] = new HashMap[String, Long]()
  private val currentJars: HashMap[String, Long] = new HashMap[String, Long]()

  private val EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new Array[Byte](0))

  private val conf = env.conf
  
  val reportedStages : ArrayBuffer[Integer] = new ArrayBuffer[Integer]()
  


  //  private var executorBackend: ExecutorBackend = null

  // No ip or host:port - just hostname
  Utils.checkHost(executorHostname, "Expected executed slave to be a hostname")
  // must not have port specified.
  assert(0 == Utils.parseHostPort(executorHostname)._2)

  // Make sure the local hostname we report matches the cluster scheduler's name for this host
  Utils.setCustomHostname(executorHostname)

  if (!isLocal) {
    // Setup an uncaught exception handler for non-local mode.
    // Make any thread terminations due to uncaught exceptions kill the entire
    // executor process to avoid surprising stalls.
    Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler)
  }

  // Count the number of tasks executed by this Executor.
  private var numberOfLaunchedTasks = 0;

  val adaptiveThreadPool = Option(System.getenv("SPARK_ADAPTIVE_THREADPOOL")).getOrElse("0").toInt
  val SPARK_HOME = Option(System.getenv("SPARK_HOME")).getOrElse("/home/omranian/scripts")
  val SCRIPT_HOME = s"$SPARK_HOME/scripts"
  println("adaptiveThreadPool: " + adaptiveThreadPool)

  val originalCores: Int = cores
  val tmpfsEnabled = 1

  val attachStraceEnabled = Option(System.getenv("SPARK_DCA_STRACE")).getOrElse("0").toInt

  def getConf(): SparkConf = {
    return conf;
  }

  private val threadPoolNoTuning = {
    val threadFactory = new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat("Executor task launch worker-%d")
      .setThreadFactory(new ThreadFactory {
        override def newThread(r: Runnable): Thread = {
          // Use UninterruptibleThread to run tasks so that we can allow running codes without being
          // interrupted by `Thread.interrupt()`. Some issues, such as KAFKA-1894, HADOOP-10622,
          // will hang forever if some methods are interrupted.
          //          println("New thread has been created!")
          new UninterruptibleThread(r, "unused") // thread name will be set by ThreadFactoryBuilder
        }
      })
      .build()
    Executors.newCachedThreadPool(threadFactory).asInstanceOf[ThreadPoolExecutor]
  }

  val staticDca = Option(System.getenv("SPARK_DCA_STATIC")).getOrElse("0").toInt
  val staticDcaThreadNum = Option(System.getenv("SPARK_DCA_STATIC_THREAD_NUM")).getOrElse("0").toInt
  // Start worker thread pool
  private val threadPool = {

    val threadFactory = new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat("Executor task launch worker-%d")
      .setThreadFactory(new ThreadFactory {
        override def newThread(r: Runnable): Thread = {
          // Use UninterruptibleThread to run tasks so that we can allow running codes without being
          // interrupted by `Thread.interrupt()`. Some issues, such as KAFKA-1894, HADOOP-10622,
          // will hang forever if some methods are interrupted.
          //          println("New thread has been created!")
          new UninterruptibleThread(r, "unused") // thread name will be set by ThreadFactoryBuilder
        }
      })
      .build()

    if (staticDca == 1) {
      new SelfAdaptiveFixedThreadPoolExecutor(
        staticDcaThreadNum,
        staticDcaThreadNum,
        60L,
        TimeUnit.SECONDS,
        new LinkedBlockingQueue[Runnable](),
        threadFactory,
        adaptiveThreadPool,
        this)
        .asInstanceOf[ThreadPoolExecutor]
    } else {
      if (adaptiveThreadPool >= 1) {
        if (adaptiveThreadPool == 100)
          new SelfAdaptiveNoActionPoolExecutor(
            0,
            Int.MaxValue,
            60L,
            TimeUnit.SECONDS,
            new SynchronousQueue[Runnable](),
            threadFactory,
            this)
            .asInstanceOf[ThreadPoolExecutor]
        else if (adaptiveThreadPool == 101)
          new SelfAdaptiveStaticThreadPoolExecutor(
            0,
            cores,
            60L,
            TimeUnit.SECONDS,
            new SynchronousQueue[Runnable](),
            threadFactory,
            this)
            .asInstanceOf[ThreadPoolExecutor]
        else
          new SelfAdaptiveThreadPoolExecutor(
            cores,
            cores,
            changedCores,
            60L,
            TimeUnit.SECONDS,
            //      new SynchronousQueue[Runnable](),
            new LinkedBlockingQueue[Runnable](),
            threadFactory,
            adaptiveThreadPool, this)
            .asInstanceOf[ThreadPoolExecutor]
      } else
        Executors.newCachedThreadPool(threadFactory).asInstanceOf[ThreadPoolExecutor]
    }
  }
  
  def addReportedStage (stageId: Integer): Int = {
    reportedStages += stageId
    1
  }
  
  def isStageReported (stageId: Integer): Boolean = {
    reportedStages.contains(stageId)
  }

  def sendThreadPoolUpdateMsg(newCoreNum: Int) {
    if (this.executorBackEnd != null) {
      logInfo(s"[MSG]: sending new threadpool core [$newCoreNum] from Executor to ExecutorBackend...")
      this.executorBackEnd.threadpoolUpdate(newCoreNum);
    }
  }

  def getPid(): Int = {
    val runtime = ManagementFactory.getRuntimeMXBean()
    val jvm = runtime.getClass().getDeclaredField("jvm");
    jvm.setAccessible(true);
    val mgmt = jvm.get(runtime).asInstanceOf[sun.management.VMManagement];
    val pid_method = mgmt.getClass().getDeclaredMethod("getProcessId");
    pid_method.setAccessible(true);

    val pid = pid_method.invoke(mgmt).asInstanceOf[Int];
    pid
  }

  def getAppName(): String = {
    conf.get("spark.app.name")
  }

  def getAppId(): String = {
    appId
  }

  def getExecutorId(): String = {
    executorId
  }

  def writeEpollWait(fileName: String, text: String) {
    import java.io._
    val pw = new PrintWriter(new File(fileName))
    pw.write(text)
    pw.close
  }

  def getStracePath: String = {

    var stracePath = ""
    if (tmpfsEnabled == 1) {
      stracePath = s"/dev/shm/log.strace"

    } else {
      val rootDir = Utils.getConfiguredLocalDirs(conf).mkString(" ");
      println(s"rootdir => ${Utils.getConfiguredLocalDirs(conf).mkString(" ")}")
      stracePath = s"$rootDir/log.strace"
    }

    return stracePath
  }

  def getIoStatPath: String = {

    var ioStatPath = ""
    if (tmpfsEnabled == 1) {
      ioStatPath = s"/dev/shm/iostat.csv"

    } else {
      val rootDir = Utils.getConfiguredLocalDirs(conf).mkString(" ");
      println(s"rootdir => ${Utils.getConfiguredLocalDirs(conf).mkString(" ")}")
      ioStatPath = s"$rootDir/iostat.csv"
    }

    return ioStatPath
  }

  def attachVtune() {
    val r = new Runnable {
      override def run() = {
        try {

          val pid = getPid()
          log.info(s"[VTUNE] attaching vtune...")
          val r = Seq("/bin/sh", "-c", s"source  /var/scratch/omranian/intel/vtune_amplifier_2019.3.0.590814/amplxe-vars.sh; amplxe-cl -collect memory-access -target-pid $pid >> /home/omranian/results/log.vtune 2>&1");
          import scala.sys.process._
          Process(r)!

          //          writeEpollWait(stracePath, "359.99")
        } catch {
          case t: Throwable => t.printStackTrace() // TODO: handle error
        }
      }
    }
    new Thread(r).start()

  }

  def attachStrace(pid: Int) {
    val r = new Runnable {
      override def run() = {
        try {
          var stracePath = getStracePath
          val cmd = s"strace -f -tt -T -e trace=lseek -o $stracePath -p $pid"
          import scala.sys.process._
          Process(cmd)!

        } catch {
          case t: Throwable => t.printStackTrace()
        }
      }
    }
    new Thread(r).start()

  }

  def attachStraceToHdfs() {
    val r = new Runnable {
      override def run() = {
        try {
          log.info(s"[HDFS-Strace] attaching strace to DataNode process...")
          val r = Seq("/bin/sh", "-c", s"/home/omranian/scripts/strace/attach_strace_to_hdfs.sh");
          import scala.sys.process._
          Process(r)!

          //          writeEpollWait(stracePath, "359.99")
        } catch {
          case t: Throwable => t.printStackTrace() // TODO: handle error
        }
      }
    }
    new Thread(r).start()

  }

  def attachStraceToPythonStdinParser(pid: Int) {
    val r = new Runnable {
      override def run() = {
        try {
          val r = Seq("/bin/sh", "-c", s"strace -f -tt -T -e trace=epoll_wait -p $pid 2>&1 | python3 /home/omranian/parser.py");

          import scala.sys.process._
          Process(r).!
        } catch {
          case t: Throwable => t.printStackTrace()
        }
      }
    }
    new Thread(r).start()

  }

  def attachStraceToPythonFileParser(pid: Int) {
    val r = new Runnable {
      override def run() = {
        try {

          var stracePath = getStracePath
          val appName = sanitizeString(getAppName())
          val appId = getAppId()
          val completeName = q"$appName^$appId"

          val r = Seq("/bin/sh", "-c", s"strace -f -tt -T -e trace=epoll_wait -o $stracePath -p $pid");

          import scala.sys.process._
          Process(r).!

        } catch {
          case t: Throwable => t.printStackTrace() // TODO: handle error
        }
      }
    }

    val r2 = new Runnable {
      override def run() = {
        try {

          var stracePath = getStracePath
          val appName = sanitizeString(getAppName())
          val appId = getAppId()
          val completeName = q"$appName^$appId"

          val r = Seq("/bin/sh", "-c", s"python3 $SCRIPT_HOME/strace/strace_file_parser.py --appName $completeName  --file $stracePath");

          import scala.sys.process._
          Process(r).!

        } catch {
          case t: Throwable => t.printStackTrace() // TODO: handle error
        }
      }
    }

    new Thread(r).start()
    new Thread(r2).start()
  }

  def startIoStat() {
    val r = new Runnable {
      override def run() = {
        try {
          var ioStatPath = getIoStatPath
          import scala.sys.process._
          //          Process(cmd)!
          val ret = s"$SCRIPT_HOME/iostat/iostat_csv.sh" #| s"tee $ioStatPath > /dev/null &" !
        } catch {
          case t: Throwable => t.printStackTrace() // TODO: handle error
        }
      }
    }
    new Thread(r).start()

  }

  def stopIoStat() {
    val r = new Runnable {
      override def run() = {
        try {
          val cmd = s"killall iostat"
          import scala.sys.process._
          Process(cmd)!
        } catch {
          case t: Throwable => t.printStackTrace() // TODO: handle error
        }
      }
    }
    new Thread(r).start()

  }

  implicit class `string quoter`(val sc: StringContext) {
    def q(args: Any*): String = "\"" + sc.s(args: _*) + "\""

  }

  def sanitizeString(s: String): String = {
    return s.replace(" ", "_").replace(",", "_").replace("(", "_").replace(")", "_").replace("/", "_")
  }

  def startPio(outputFileNameArg: String) {
    val r = new Runnable {
      override def run() = {
        try {
          var outputFileName = sanitizeString(outputFileNameArg)
          val pid = getPid()
          val appName = sanitizeString(getAppName())
          val appId = getAppId()
          val completeName = q"$appName^$appId"
          val execId = getExecutorId()
          log.info(s"[PIO] Starting pio on process [$pid], output file name: $outputFileName");

          val r = Seq("/bin/sh", "-c", s"$SCRIPT_HOME/table2_experiment[io_activity]/pio_executor_hdfs.sh $pid $outputFileName $completeName $execId");

          import scala.sys.process._
          Process(r).!
        } catch {
          case t: Throwable => t.printStackTrace() // TODO: handle error
        }
      }
    }
    new Thread(r).start()

  }

  def startMpstat() {
    val r = new Runnable {
      override def run() = {
        try {
          val appName = sanitizeString(getAppName())
          val appId = getAppId()
          val completeName = q"$appName^$appId"
          val execId = getExecutorId()
          log.info(s"[MPSTAT] Starting mpstat 1, output file name: $completeName");

          val r = Seq("/bin/sh", "-c", s"mpstat 1 | python3 $SCRIPT_HOME/figure1_experiment[io_wait_time]/parse-mpstat-output.py -e $execId -a $completeName -f $completeName");

          import scala.sys.process._
          Process(r).!
        } catch {
          case t: Throwable => t.printStackTrace() // TODO: handle error
        }
      }
    }
    new Thread(r).start()

  }

  def startIoStatMonitoring() {
    val r = new Runnable {
      override def run() = {
        try {
          val appName = sanitizeString(getAppName())
          val appId = getAppId()
          val completeName = q"$appName^$appId"
          log.info(s"[IOSTAT(M)] Starting iostat, output file name: $completeName");

          val r = Seq("/bin/sh", "-c", s"iostat -c -d -t -x -y 1 | awk -f $SCRIPT_HOME/iostat/buffer.awk | python3 $SCRIPT_HOME/iostat/parse-iostat-output.py -a $completeName -f $completeName -d sda -d sdb");

          import scala.sys.process._
          Process(r).!
        } catch {
          case t: Throwable => t.printStackTrace() // TODO: handle error
        }
      }
    }
    new Thread(r).start()

  }

  def startTop() {
    val r = new Runnable {
      override def run() = {
        try {
          val appName = sanitizeString(getAppName())
          val appId = getAppId()
          val completeName = q"$appName^$appId"
          val execId = getExecutorId()
          log.info(s"[TOP] Starting top, output file name: $completeName");

          val pid = getPid()

          val r = Seq("/bin/sh", "-c", s"top -b  -c -p $pid | python3 $SCRIPT_HOME/figure1_experiment[io_wait_time]/parse-top-output.py -e $execId -a $completeName -f $completeName");

          import scala.sys.process._
          Process(r).!
        } catch {
          case t: Throwable => t.printStackTrace() // TODO: handle error
        }
      }
    }
    new Thread(r).start()
  }

  def startMemStall() {
    val r = new Runnable {
      override def run() = {
        try {
          val appName = sanitizeString(getAppName())
          val appId = getAppId()
          val completeName = q"$appName^$appId"
          log.info(s"[MEM-STALL] Starting perf for memeory stalls, output file name: $completeName");

          val pid = getPid()
          val perfEvents = "cycle_activity.cycles_no_execute,cycle_activity.stalls_ldm_pending,CPU_CLK_UNHALTED.REF_TSC"
          val r = Seq("/bin/sh", "-c", s"perf stat -I 1000 -x,  -a  -e  $perfEvents -p $pid 2>&1 | python3 /home/omranian/scripts/memstall/parse-memstall-output.py -a $completeName -f $completeName");

          import scala.sys.process._
          Process(r).!
        } catch {
          case t: Throwable => t.printStackTrace() // TODO: handle error
        }
      }
    }
    new Thread(r).start()

  }

  if (getIoStatPath != "") {
    println(s"Starting iostat...")
    startIoStat();
  }

  private val executorSource = new ExecutorSource(threadPool, executorId)
  private val executorSourceNoTuning = new ExecutorSource(threadPoolNoTuning, executorId)
  // Pool used for threads that supervise task killing / cancellation
  private val taskReaperPool = ThreadUtils.newDaemonCachedThreadPool("Task reaper")
  // For tasks which are in the process of being killed, this map holds the most recently created
  // TaskReaper. All accesses to this map should be synchronized on the map itself (this isn't
  // a ConcurrentHashMap because we use the synchronization for purposes other than simply guarding
  // the integrity of the map's internal state). The purpose of this map is to prevent the creation
  // of a separate TaskReaper for every killTask() of a given task. Instead, this map allows us to
  // track whether an existing TaskReaper fulfills the role of a TaskReaper that we would otherwise
  // create. The map key is a task id.
  private val taskReaperForTask: HashMap[Long, TaskReaper] = HashMap[Long, TaskReaper]()

  if (!isLocal) {
    env.metricsSystem.registerSource(executorSource)
    //    if (staticDca == 1)
    //      env.metricsSystem.registerSource(executorSourceNoTuning)
    env.blockManager.initialize(conf.getAppId)
  }

  // Whether to load classes in user jars before those in Spark jars
  private val userClassPathFirst = conf.getBoolean("spark.executor.userClassPathFirst", false)

  // Whether to monitor killed / interrupted tasks
  private val taskReaperEnabled = conf.getBoolean("spark.task.reaper.enabled", false)

  // Create our ClassLoader
  // do this after SparkEnv creation so can access the SecurityManager
  private val urlClassLoader = createClassLoader()
  private val replClassLoader = addReplClassLoaderIfNeeded(urlClassLoader)

  // Set the classloader for serializer
  env.serializer.setDefaultClassLoader(replClassLoader)
  // SPARK-21928.  SerializerManager's internal instance of Kryo might get used in netty threads
  // for fetching remote cached RDD blocks, so need to make sure it uses the right classloader too.
  env.serializerManager.setDefaultClassLoader(replClassLoader)

  // Max size of direct result. If task result is bigger than this, we use the block manager
  // to send the result back.
  private val maxDirectResultSize = Math.min(
    conf.getSizeAsBytes("spark.task.maxDirectResultSize", 1L << 20),
    RpcUtils.maxMessageSizeBytes(conf))

  // Limit of bytes for total size of results (default is 1GB)
  private val maxResultSize = Utils.getMaxResultSize(conf)

  // Maintains the list of running tasks.
  private val runningTasks = new ConcurrentHashMap[Long, TaskRunner]

  // Executor for the heartbeat task.
  private val heartbeater = ThreadUtils.newDaemonSingleThreadScheduledExecutor("driver-heartbeater")

  // must be initialized before running startDriverHeartbeat()
  private val heartbeatReceiverRef =
    RpcUtils.makeDriverRef(HeartbeatReceiver.ENDPOINT_NAME, conf, env.rpcEnv)

  /**
   * When an executor is unable to send heartbeats to the driver more than `HEARTBEAT_MAX_FAILURES`
   * times, it should kill itself. The default value is 60. It means we will retry to send
   * heartbeats about 10 minutes because the heartbeat interval is 10s.
   */
  private val HEARTBEAT_MAX_FAILURES = conf.getInt("spark.executor.heartbeat.maxFailures", 60)

  /**
   * Count the failure times of heartbeat. It should only be accessed in the heartbeat thread. Each
   * successful heartbeat will reset it to 0.
   */
  private var heartbeatFailures = 0

  startDriverHeartbeater()

  private[executor] def numRunningTasks: Int = runningTasks.size()

  var stageId: Int = -1
  var isCurrentStageIo: Boolean = false

  def launchTask(context: ExecutorBackend, taskDescription: TaskDescription): Unit = {

    def getFullAppName(tr: TaskRunner): String = {
      val appName = sanitizeString(tr.getAppName());
      val appId = tr.getAppId();
      val executorId = tr.getExecutorId();
      val finalName = appName + "-" + appId + "#" + executorId;
      finalName
    }
    //    numberOfLaunchedTasks += 1;
    //    val ioParallelism = Option(System.getenv("SPARK_DCA_PARALLELISM")).getOrElse("0").toInt
    ////    println(s"number of launched tasks: $numberOfLaunchedTasks")
    //    if(numberOfLaunchedTasks % ioParallelism == 0){
    //      taskDescription.shouldOptimise = true;
    //      println(s"Setting task [${taskDescription.taskId}] should optimise to true");
    //    }
    val tr = new TaskRunner(context, taskDescription)
    runningTasks.put(taskDescription.taskId, tr)
    isCurrentStageIo = taskDescription.isIo

    if (staticDca == 1) {
      if (stageId != taskDescription.stageId) {
        logInfo("[CRITICAL] Stage change detected in Executor!")
        stageId = taskDescription.stageId
        logInfo(s"[CRITICAL] taskDescription.isIo: ${taskDescription.isIo}")
        val finalName = getFullAppName(tr)
        if (!taskDescription.isIo) {
          logInfo("[CRITICAL] Manually letting the sockets know stage has changed it is not an IO phase!")
          (threadPool.asInstanceOf[SelfAdaptiveFixedThreadPoolExecutor]).notifySockets(stageId);
          logInfo("[CRITICAL] Resetting the number of cores since it is not an IO phase!")
          sendThreadPoolUpdateMsg(cores)
          logInfo("[CRITICAL] Saving DCA results for this non-io stage!")
          (threadPool.asInstanceOf[SelfAdaptiveFixedThreadPoolExecutor]).saveDca(stageId, finalName)
        } else {
          logInfo(s"[CRITICAL] Setting the number of cores in scheduler to $staticDcaThreadNum since it is an IO phase!")
          sendThreadPoolUpdateMsg(staticDcaThreadNum)
          (threadPool.asInstanceOf[SelfAdaptiveFixedThreadPoolExecutor]).saveDcaIo(stageId, finalName)
        }
      }

      if (taskDescription.isIo) {
        threadPool.execute(tr)
      } else {
        threadPoolNoTuning.execute(tr)
      }

    } else {
      // if default mode, save dca.
      if (stageId != taskDescription.stageId) {
        stageId = taskDescription.stageId
        logInfo("[CRITICAL] Stage change detected in Executor!")
        if (adaptiveThreadPool == 100) {
          val finalName = getFullAppName(tr)
          (threadPool.asInstanceOf[SelfAdaptiveNoActionPoolExecutor]).saveDca(stageId, finalName)
        }
      }
      threadPool.execute(tr)
    }

  }

  def killTask(taskId: Long, interruptThread: Boolean, reason: String): Unit = {
    val taskRunner = runningTasks.get(taskId)
    if (taskRunner != null) {
      if (taskReaperEnabled) {
        val maybeNewTaskReaper: Option[TaskReaper] = taskReaperForTask.synchronized {
          val shouldCreateReaper = taskReaperForTask.get(taskId) match {
            case None                 => true
            case Some(existingReaper) => interruptThread && !existingReaper.interruptThread
          }
          if (shouldCreateReaper) {
            val taskReaper = new TaskReaper(
              taskRunner, interruptThread = interruptThread, reason = reason)
            taskReaperForTask(taskId) = taskReaper
            Some(taskReaper)
          } else {
            None
          }
        }
        // Execute the TaskReaper from outside of the synchronized block.
        maybeNewTaskReaper.foreach(taskReaperPool.execute)
      } else {
        taskRunner.kill(interruptThread = interruptThread, reason = reason)
      }
    }
  }

  /**
   * Function to kill the running tasks in an executor.
   * This can be called by executor back-ends to kill the
   * tasks instead of taking the JVM down.
   * @param interruptThread whether to interrupt the task thread
   */
  def killAllTasks(interruptThread: Boolean, reason: String): Unit = {
    runningTasks.keys().asScala.foreach(t =>
      killTask(t, interruptThread = interruptThread, reason = reason))
  }

  def stop(): Unit = {
    env.metricsSystem.report()
    heartbeater.shutdown()
    heartbeater.awaitTermination(10, TimeUnit.SECONDS)
    threadPool.shutdown()
    threadPoolNoTuning.shutdown()
    stopIoStat()
    if (!isLocal) {
      env.stop()
    }
  }

  /** Returns the total amount of time this JVM process has spent in garbage collection. */
  private def computeTotalGcTime(): Long = {
    ManagementFactory.getGarbageCollectorMXBeans.asScala.map(_.getCollectionTime).sum
  }

  class TaskRunner(
    execBackend:                 ExecutorBackend,
    private val taskDescription: TaskDescription)
    extends Runnable {

    //    if (executorBackend == null)
    //      executorBackend = execBackend;

    var taskStart: Long = 0
    val taskId = taskDescription.taskId
    val threadName = s"Executor task launch worker for task $taskId"
    private val taskName = taskDescription.name

    /** If specified, this task has been killed and this option contains the reason. */
    @volatile private var reasonIfKilled: Option[String] = None

    @volatile private var threadId: Long = -1

    def getThreadId: Long = threadId

    /** Whether this task has been finished. */
    @GuardedBy("TaskRunner.this")
    private var finished = false

    def isFinished: Boolean = synchronized { finished }

    /** How much the JVM process has spent in GC when the task starts to run. */
    @volatile var startGCTime: Long = _

    /**
     * The task to run. This will be set in run() by deserializing the task binary coming
     * from the driver. Once it is set, it will never be changed.
     */
    @volatile var task: Task[Any] = _

    def kill(interruptThread: Boolean, reason: String): Unit = {
      logInfo(s"Executor is trying to kill $taskName (TID $taskId), reason: $reason")
      reasonIfKilled = Some(reason)
      if (task != null) {
        synchronized {
          if (!finished) {
            task.kill(interruptThread, reason)
          }
        }
      }
    }

    //    def getStracePath(): String = {
    //      val rootDir = Utils.getConfiguredLocalDirs(conf).mkString(" ");
    //      val stracePath = s"$rootDir/log.strace"
    //      stracePath
    //    }
    //
    //    def getIoStatPath(): String = {
    //      val rootDir = Utils.getConfiguredLocalDirs(conf).mkString(" ");
    //      val stracePath = s"$rootDir/iostat.csv"
    //      stracePath
    //
    //    }

    def getAppName(): String = {
      conf.get("spark.app.name")
    }

    def getAppId(): String = {
      taskDescription.applicationId
    }

    def getExecutorId(): String = {
      executorId
    }

    def getStageId(): Int = {
      taskDescription.stageId
    }

    def getStartTime(): Long = {
      taskStart
    }

    def getCurrentExecutionTime(): Long = {
      System.currentTimeMillis() - taskStart
    }

    def getExecutorRunTime(): Long = {
      task.metrics.executorRunTime
    }

    def getMetrics(): TaskMetrics = {
      task.metrics
    }

    def isTaskIo(): Boolean = {
      taskDescription.isIo
    }

    def getBytesRead(): Long = {
      try {
        task.metrics.inputMetrics.bytesRead
      } catch {
        case t: Exception => return 0
      }
    }

    def getBytesReadAll(): Long = {
      var result = 0L
      try {
        result += task.metrics.inputMetrics.bytesRead
      } catch {
        case t: Exception => result += 0L
      }
      try {
        result += task.metrics.shuffleReadMetrics.localBytesRead
      } catch {
        case t: Exception => result += 0L
      }
      try {
        result += task.metrics.shuffleReadMetrics.remoteBytesRead
      } catch {
        case t: Exception => result += 0L
      }
      return result
    }

    def getBytesWrittenAll(): Long = {
      var result = 0L
      try {
        result += task.metrics.outputMetrics.bytesWritten
      } catch {
        case t: Exception => result += 0L
      }
      try {
        result += task.metrics.shuffleWriteMetrics.bytesWritten
      } catch {
        case t: Exception => result += 0L
      }
      return result
    }

    def getBytesWritten(): Long = {
      try {
        task.metrics.shuffleWriteMetrics.bytesWritten
      } catch {
        case t: Exception => return 0
      }
    }

    def getGcTime(): Long = {
      try {
        task.metrics.jvmGCTime
      } catch {
        case t: Exception => return 0
      }
    }

    //    def getBytesReadAndWritten(): Long = {
    //      try {
    //        task.metrics.inputMetrics.bytesRead + task.metrics.outputMetrics.bytesWritten
    //      } catch {
    //        case t: Exception => return 0
    //      }
    //    }

    def shouldOptimise: Boolean = taskDescription.shouldOptimise

    /**
     * Set the finished flag to true and clear the current thread's interrupt status
     */
    private def setTaskFinishedAndClearInterruptStatus(): Unit = synchronized {
      this.finished = true
      // SPARK-14234 - Reset the interrupted status of the thread to avoid the
      // ClosedByInterruptException during execBackend.statusUpdate which causes
      // Executor to crash
      Thread.interrupted()
      // Notify any waiting TaskReapers. Generally there will only be one reaper per task but there
      // is a rare corner-case where one task can have two reapers in case cancel(interrupt=False)
      // is followed by cancel(interrupt=True). Thus we use notifyAll() to avoid a lost wakeup:
      notifyAll()
    }

    override def run(): Unit = {
      threadId = Thread.currentThread.getId
      Thread.currentThread.setName(threadName)
      val threadMXBean = ManagementFactory.getThreadMXBean
      val taskMemoryManager = new TaskMemoryManager(env.memoryManager, taskId)
      val deserializeStartTime = System.currentTimeMillis()
      val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
        threadMXBean.getCurrentThreadCpuTime
      } else 0L
      Thread.currentThread.setContextClassLoader(replClassLoader)
      val ser = env.closureSerializer.newInstance()
      logInfo(s"Running $taskName (TID $taskId)")
      execBackend.statusUpdate(taskId, TaskState.RUNNING, EMPTY_BYTE_BUFFER)

      var taskStartCpu: Long = 0
      startGCTime = computeTotalGcTime()

      try {
        // Must be set before updateDependencies() is called, in case fetching dependencies
        // requires access to properties contained within (e.g. for access control).
        Executor.taskDeserializationProps.set(taskDescription.properties)

        updateDependencies(taskDescription.addedFiles, taskDescription.addedJars)
        task = ser.deserialize[Task[Any]](
          taskDescription.serializedTask, Thread.currentThread.getContextClassLoader)
        task.localProperties = taskDescription.properties
        task.setTaskMemoryManager(taskMemoryManager)

        // If this task has been killed before we deserialized it, let's quit now. Otherwise,
        // continue executing the task.
        val killReason = reasonIfKilled
        if (killReason.isDefined) {
          // Throw an exception rather than returning, because returning within a try{} block
          // causes a NonLocalReturnControl exception to be thrown. The NonLocalReturnControl
          // exception will be caught by the catch block, leading to an incorrect ExceptionFailure
          // for the task.
          throw new TaskKilledException(killReason.get)
        }

        // The purpose of updating the epoch here is to invalidate executor map output status cache
        // in case FetchFailures have occurred. In local mode `env.mapOutputTracker` will be
        // MapOutputTrackerMaster and its cache invalidation is not based on epoch numbers so
        // we don't need to make any special calls here.
        if (!isLocal) {
          logDebug("Task " + taskId + "'s epoch is " + task.epoch)
          env.mapOutputTracker.asInstanceOf[MapOutputTrackerWorker].updateEpoch(task.epoch)
        }

        // Run the actual task and measure its runtime.
        taskStart = System.currentTimeMillis()
        taskStartCpu = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
          threadMXBean.getCurrentThreadCpuTime
        } else 0L
        var threwException = true
        val value = try {
          val res = task.run(
            taskAttemptId = taskId,
            attemptNumber = taskDescription.attemptNumber,
            metricsSystem = env.metricsSystem)
          threwException = false
          res
        } finally {
          val releasedLocks = env.blockManager.releaseAllLocksForTask(taskId)
          val freedMemory = taskMemoryManager.cleanUpAllAllocatedMemory()

          if (freedMemory > 0 && !threwException) {
            val errMsg = s"Managed memory leak detected; size = $freedMemory bytes, TID = $taskId"
            if (conf.getBoolean("spark.unsafe.exceptionOnMemoryLeak", false)) {
              throw new SparkException(errMsg)
            } else {
              logWarning(errMsg)
            }
          }

          if (releasedLocks.nonEmpty && !threwException) {
            val errMsg =
              s"${releasedLocks.size} block locks were not released by TID = $taskId:\n" +
                releasedLocks.mkString("[", ", ", "]")
            if (conf.getBoolean("spark.storage.exceptionOnPinLeak", false)) {
              throw new SparkException(errMsg)
            } else {
              logInfo(errMsg)
            }
          }
        }
        task.context.fetchFailed.foreach { fetchFailure =>
          // uh-oh.  it appears the user code has caught the fetch-failure without throwing any
          // other exceptions.  Its *possible* this is what the user meant to do (though highly
          // unlikely).  So we will log an error and keep going.
          logError(s"TID ${taskId} completed successfully though internally it encountered " +
            s"unrecoverable fetch failures!  Most likely this means user code is incorrectly " +
            s"swallowing Spark's internal ${classOf[FetchFailedException]}", fetchFailure)
        }
        val taskFinish = System.currentTimeMillis()
        val taskFinishCpu = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
          threadMXBean.getCurrentThreadCpuTime
        } else 0L

        // If the task has been killed, let's fail it.
        task.context.killTaskIfInterrupted()

        val resultSer = env.serializer.newInstance()
        val beforeSerialization = System.currentTimeMillis()
        val valueBytes = resultSer.serialize(value)
        val afterSerialization = System.currentTimeMillis()

        // Deserialization happens in two parts: first, we deserialize a Task object, which
        // includes the Partition. Second, Task.run() deserializes the RDD and function to be run.
        task.metrics.setExecutorDeserializeTime(
          (taskStart - deserializeStartTime) + task.executorDeserializeTime)
        task.metrics.setExecutorDeserializeCpuTime(
          (taskStartCpu - deserializeStartCpuTime) + task.executorDeserializeCpuTime)
        // We need to subtract Task.run()'s deserialization time to avoid double-counting
        task.metrics.setExecutorRunTime((taskFinish - taskStart) - task.executorDeserializeTime)
        task.metrics.setExecutorCpuTime(
          (taskFinishCpu - taskStartCpu) - task.executorDeserializeCpuTime)
        task.metrics.setJvmGCTime(computeTotalGcTime() - startGCTime)
        task.metrics.setResultSerializationTime(afterSerialization - beforeSerialization)

        // Note: accumulator updates must be collected after TaskMetrics is updated
        val accumUpdates = task.collectAccumulatorUpdates()
        // TODO: do not serialize value twice
        val directResult = new DirectTaskResult(valueBytes, accumUpdates)
        val serializedDirectResult = ser.serialize(directResult)
        val resultSize = serializedDirectResult.limit

        // directSend = sending directly back to the driver
        val serializedResult: ByteBuffer = {
          if (maxResultSize > 0 && resultSize > maxResultSize) {
            logWarning(s"Finished $taskName (TID $taskId). Result is larger than maxResultSize " +
              s"(${Utils.bytesToString(resultSize)} > ${Utils.bytesToString(maxResultSize)}), " +
              s"dropping it.")
            ser.serialize(new IndirectTaskResult[Any](TaskResultBlockId(taskId), resultSize))
          } else if (resultSize > maxDirectResultSize) {
            val blockId = TaskResultBlockId(taskId)
            env.blockManager.putBytes(
              blockId,
              new ChunkedByteBuffer(serializedDirectResult.duplicate()),
              StorageLevel.MEMORY_AND_DISK_SER)
            logInfo(
              s"Finished $taskName (TID $taskId). $resultSize bytes result sent via BlockManager)")
            ser.serialize(new IndirectTaskResult[Any](blockId, resultSize))
          } else {
            logInfo(s"Finished $taskName (TID $taskId). $resultSize bytes result sent to driver")
            serializedDirectResult
          }
        }

        setTaskFinishedAndClearInterruptStatus()
        execBackend.statusUpdate(taskId, TaskState.FINISHED, serializedResult)

      } catch {
        case t: TaskKilledException =>
          logInfo(s"Executor killed $taskName (TID $taskId), reason: ${t.reason}")
          setTaskFinishedAndClearInterruptStatus()
          execBackend.statusUpdate(taskId, TaskState.KILLED, ser.serialize(TaskKilled(t.reason)))

        case _: InterruptedException | NonFatal(_) if task != null && task.reasonIfKilled.isDefined =>
          val killReason = task.reasonIfKilled.getOrElse("unknown reason")
          logInfo(s"Executor interrupted and killed $taskName (TID $taskId), reason: $killReason")
          setTaskFinishedAndClearInterruptStatus()
          execBackend.statusUpdate(
            taskId, TaskState.KILLED, ser.serialize(TaskKilled(killReason)))

        case t: Throwable if hasFetchFailure && !Utils.isFatalError(t) =>
          val reason = task.context.fetchFailed.get.toTaskFailedReason
          if (!t.isInstanceOf[FetchFailedException]) {
            // there was a fetch failure in the task, but some user code wrapped that exception
            // and threw something else.  Regardless, we treat it as a fetch failure.
            val fetchFailedCls = classOf[FetchFailedException].getName
            logWarning(s"TID ${taskId} encountered a ${fetchFailedCls} and " +
              s"failed, but the ${fetchFailedCls} was hidden by another " +
              s"exception.  Spark is handling this like a fetch failure and ignoring the " +
              s"other exception: $t")
          }
          setTaskFinishedAndClearInterruptStatus()
          execBackend.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))

        case CausedBy(cDE: CommitDeniedException) =>
          val reason = cDE.toTaskFailedReason
          setTaskFinishedAndClearInterruptStatus()
          execBackend.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))

        case t: Throwable =>
          // Attempt to exit cleanly by informing the driver of our failure.
          // If anything goes wrong (or this was a fatal exception), we will delegate to
          // the default uncaught exception handler, which will terminate the Executor.
          logError(s"Exception in $taskName (TID $taskId)", t)

          // SPARK-20904: Do not report failure to driver if if happened during shut down. Because
          // libraries may set up shutdown hooks that race with running tasks during shutdown,
          // spurious failures may occur and can result in improper accounting in the driver (e.g.
          // the task failure would not be ignored if the shutdown happened because of premption,
          // instead of an app issue).
          if (!ShutdownHookManager.inShutdown()) {
            // Collect latest accumulator values to report back to the driver
            val accums: Seq[AccumulatorV2[_, _]] =
              if (task != null) {
                task.metrics.setExecutorRunTime(System.currentTimeMillis() - taskStart)
                task.metrics.setJvmGCTime(computeTotalGcTime() - startGCTime)
                task.collectAccumulatorUpdates(taskFailed = true)
              } else {
                Seq.empty
              }

            val accUpdates = accums.map(acc => acc.toInfo(Some(acc.value), None))

            val serializedTaskEndReason = {
              try {
                ser.serialize(new ExceptionFailure(t, accUpdates).withAccums(accums))
              } catch {
                case _: NotSerializableException =>
                  // t is not serializable so just send the stacktrace
                  ser.serialize(new ExceptionFailure(t, accUpdates, false).withAccums(accums))
              }
            }
            setTaskFinishedAndClearInterruptStatus()
            execBackend.statusUpdate(taskId, TaskState.FAILED, serializedTaskEndReason)
          } else {
            logInfo("Not reporting error to driver during JVM shutdown.")
          }

          // Don't forcibly exit unless the exception was inherently fatal, to avoid
          // stopping other tasks unnecessarily.
          if (Utils.isFatalError(t)) {
            uncaughtExceptionHandler.uncaughtException(Thread.currentThread(), t)
          }

      } finally {
        runningTasks.remove(taskId)
      }
    }

    private def hasFetchFailure: Boolean = {
      task != null && task.context != null && task.context.fetchFailed.isDefined
    }
  }

  /**
   * Supervises the killing / cancellation of a task by sending the interrupted flag, optionally
   * sending a Thread.interrupt(), and monitoring the task until it finishes.
   *
   * Spark's current task cancellation / task killing mechanism is "best effort" because some tasks
   * may not be interruptable or may not respond to their "killed" flags being set. If a significant
   * fraction of a cluster's task slots are occupied by tasks that have been marked as killed but
   * remain running then this can lead to a situation where new jobs and tasks are starved of
   * resources that are being used by these zombie tasks.
   *
   * The TaskReaper was introduced in SPARK-18761 as a mechanism to monitor and clean up zombie
   * tasks. For backwards-compatibility / backportability this component is disabled by default
   * and must be explicitly enabled by setting `spark.task.reaper.enabled=true`.
   *
   * A TaskReaper is created for a particular task when that task is killed / cancelled. Typically
   * a task will have only one TaskReaper, but it's possible for a task to have up to two reapers
   * in case kill is called twice with different values for the `interrupt` parameter.
   *
   * Once created, a TaskReaper will run until its supervised task has finished running. If the
   * TaskReaper has not been configured to kill the JVM after a timeout (i.e. if
   * `spark.task.reaper.killTimeout < 0`) then this implies that the TaskReaper may run indefinitely
   * if the supervised task never exits.
   */
  private class TaskReaper(
    taskRunner:          TaskRunner,
    val interruptThread: Boolean,
    val reason:          String)
    extends Runnable {

    private[this] val taskId: Long = taskRunner.taskId

    private[this] val killPollingIntervalMs: Long =
      conf.getTimeAsMs("spark.task.reaper.pollingInterval", "10s")

    private[this] val killTimeoutMs: Long = conf.getTimeAsMs("spark.task.reaper.killTimeout", "-1")

    private[this] val takeThreadDump: Boolean =
      conf.getBoolean("spark.task.reaper.threadDump", true)

    override def run(): Unit = {
      val startTimeMs = System.currentTimeMillis()
      def elapsedTimeMs = System.currentTimeMillis() - startTimeMs
      def timeoutExceeded(): Boolean = killTimeoutMs > 0 && elapsedTimeMs > killTimeoutMs
      try {
        // Only attempt to kill the task once. If interruptThread = false then a second kill
        // attempt would be a no-op and if interruptThread = true then it may not be safe or
        // effective to interrupt multiple times:
        taskRunner.kill(interruptThread = interruptThread, reason = reason)
        // Monitor the killed task until it exits. The synchronization logic here is complicated
        // because we don't want to synchronize on the taskRunner while possibly taking a thread
        // dump, but we also need to be careful to avoid races between checking whether the task
        // has finished and wait()ing for it to finish.
        var finished: Boolean = false
        while (!finished && !timeoutExceeded()) {
          taskRunner.synchronized {
            // We need to synchronize on the TaskRunner while checking whether the task has
            // finished in order to avoid a race where the task is marked as finished right after
            // we check and before we call wait().
            if (taskRunner.isFinished) {
              finished = true
            } else {
              taskRunner.wait(killPollingIntervalMs)
            }
          }
          if (taskRunner.isFinished) {
            finished = true
          } else {
            logWarning(s"Killed task $taskId is still running after $elapsedTimeMs ms")
            if (takeThreadDump) {
              try {
                Utils.getThreadDumpForThread(taskRunner.getThreadId).foreach { thread =>
                  if (thread.threadName == taskRunner.threadName) {
                    logWarning(s"Thread dump from task $taskId:\n${thread.stackTrace}")
                  }
                }
              } catch {
                case NonFatal(e) =>
                  logWarning("Exception thrown while obtaining thread dump: ", e)
              }
            }
          }
        }

        if (!taskRunner.isFinished && timeoutExceeded()) {
          if (isLocal) {
            logError(s"Killed task $taskId could not be stopped within $killTimeoutMs ms; " +
              "not killing JVM because we are running in local mode.")
          } else {
            // In non-local-mode, the exception thrown here will bubble up to the uncaught exception
            // handler and cause the executor JVM to exit.
            throw new SparkException(
              s"Killing executor JVM because killed task $taskId could not be stopped within " +
                s"$killTimeoutMs ms.")
          }
        }
      } finally {
        // Clean up entries in the taskReaperForTask map.
        taskReaperForTask.synchronized {
          taskReaperForTask.get(taskId).foreach { taskReaperInMap =>
            if (taskReaperInMap eq this) {
              taskReaperForTask.remove(taskId)
            } else {
              // This must have been a TaskReaper where interruptThread == false where a subsequent
              // killTask() call for the same task had interruptThread == true and overwrote the
              // map entry.
            }
          }
        }
      }
    }
  }

  /**
   * Create a ClassLoader for use in tasks, adding any JARs specified by the user or any classes
   * created by the interpreter to the search path
   */
  private def createClassLoader(): MutableURLClassLoader = {
    // Bootstrap the list of jars with the user class path.
    val now = System.currentTimeMillis()
    userClassPath.foreach { url =>
      currentJars(url.getPath().split("/").last) = now
    }

    val currentLoader = Utils.getContextOrSparkClassLoader

    // For each of the jars in the jarSet, add them to the class loader.
    // We assume each of the files has already been fetched.
    val urls = userClassPath.toArray ++ currentJars.keySet.map { uri =>
      new File(uri.split("/").last).toURI.toURL
    }
    if (userClassPathFirst) {
      new ChildFirstURLClassLoader(urls, currentLoader)
    } else {
      new MutableURLClassLoader(urls, currentLoader)
    }
  }

  /**
   * If the REPL is in use, add another ClassLoader that will read
   * new classes defined by the REPL as the user types code
   */
  private def addReplClassLoaderIfNeeded(parent: ClassLoader): ClassLoader = {
    val classUri = conf.get("spark.repl.class.uri", null)
    if (classUri != null) {
      logInfo("Using REPL class URI: " + classUri)
      try {
        val _userClassPathFirst: java.lang.Boolean = userClassPathFirst
        val klass = Utils.classForName("org.apache.spark.repl.ExecutorClassLoader")
          .asInstanceOf[Class[_ <: ClassLoader]]
        val constructor = klass.getConstructor(classOf[SparkConf], classOf[SparkEnv],
          classOf[String], classOf[ClassLoader], classOf[Boolean])
        constructor.newInstance(conf, env, classUri, parent, _userClassPathFirst)
      } catch {
        case _: ClassNotFoundException =>
          logError("Could not find org.apache.spark.repl.ExecutorClassLoader on classpath!")
          System.exit(1)
          null
      }
    } else {
      parent
    }
  }

  /**
   * Download any missing dependencies if we receive a new set of files and JARs from the
   * SparkContext. Also adds any new JARs we fetched to the class loader.
   */
  private def updateDependencies(newFiles: Map[String, Long], newJars: Map[String, Long]) {
    lazy val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    synchronized {
      // Fetch missing dependencies
      for ((name, timestamp) <- newFiles if currentFiles.getOrElse(name, -1L) < timestamp) {
        logInfo("Fetching " + name + " with timestamp " + timestamp)
        // Fetch file with useCache mode, close cache for local mode.
        Utils.fetchFile(name, new File(SparkFiles.getRootDirectory()), conf,
          env.securityManager, hadoopConf, timestamp, useCache = !isLocal)
        currentFiles(name) = timestamp
      }
      for ((name, timestamp) <- newJars) {
        val localName = new URI(name).getPath.split("/").last
        val currentTimeStamp = currentJars.get(name)
          .orElse(currentJars.get(localName))
          .getOrElse(-1L)
        if (currentTimeStamp < timestamp) {
          logInfo("Fetching " + name + " with timestamp " + timestamp)
          // Fetch file with useCache mode, close cache for local mode.
          Utils.fetchFile(name, new File(SparkFiles.getRootDirectory()), conf,
            env.securityManager, hadoopConf, timestamp, useCache = !isLocal)
          currentJars(name) = timestamp
          // Add it to our class loader
          val url = new File(SparkFiles.getRootDirectory(), localName).toURI.toURL
          if (!urlClassLoader.getURLs().contains(url)) {
            logInfo("Adding " + url + " to class loader")
            urlClassLoader.addURL(url)
          }
        }
      }
    }
  }

  /** Reports heartbeat and metrics for active tasks to the driver. */
  private def reportHeartBeat(): Unit = {
    // list of (task id, accumUpdates) to send back to the driver
    val accumUpdates = new ArrayBuffer[(Long, Seq[AccumulatorV2[_, _]])]()
    val curGCTime = computeTotalGcTime()

    for (taskRunner <- runningTasks.values().asScala) {
      if (taskRunner.task != null) {
        taskRunner.task.metrics.mergeShuffleReadMetrics()
        taskRunner.task.metrics.setJvmGCTime(curGCTime - taskRunner.startGCTime)
        accumUpdates += ((taskRunner.taskId, taskRunner.task.metrics.accumulators()))
      }
    }

    val message = Heartbeat(executorId, accumUpdates.toArray, env.blockManager.blockManagerId)
    try {
      val response = heartbeatReceiverRef.askSync[HeartbeatResponse](
        message, RpcTimeout(conf, "spark.executor.heartbeatInterval", "10s"))
      if (response.reregisterBlockManager) {
        logInfo("Told to re-register on heartbeat")
        env.blockManager.reregister()
      }
      heartbeatFailures = 0
    } catch {
      case NonFatal(e) =>
        logWarning("Issue communicating with driver in heartbeater", e)
        heartbeatFailures += 1
        if (heartbeatFailures >= HEARTBEAT_MAX_FAILURES) {
          logError(s"Exit as unable to send heartbeats to driver " +
            s"more than $HEARTBEAT_MAX_FAILURES times")
          System.exit(ExecutorExitCode.HEARTBEAT_FAILURE)
        }
    }
  }

  /**
   * Schedules a task to report heartbeat and partial metrics for active tasks to driver.
   */
  private def startDriverHeartbeater(): Unit = {
    val intervalMs = conf.getTimeAsMs("spark.executor.heartbeatInterval", "10s")

    // Wait a random interval so the heartbeats don't end up in sync
    val initialDelay = intervalMs + (math.random * intervalMs).asInstanceOf[Int]

    val heartbeatTask = new Runnable() {
      override def run(): Unit = Utils.logUncaughtExceptions(reportHeartBeat())
    }
    heartbeater.scheduleAtFixedRate(heartbeatTask, initialDelay, intervalMs, TimeUnit.MILLISECONDS)
  }
}

private[spark] object Executor {
  // This is reserved for internal use by components that need to read task properties before a
  // task is fully deserialized. When possible, the TaskContext.getLocalProperty call should be
  // used instead.
  val taskDeserializationProps: ThreadLocal[Properties] = new ThreadLocal[Properties]
}
