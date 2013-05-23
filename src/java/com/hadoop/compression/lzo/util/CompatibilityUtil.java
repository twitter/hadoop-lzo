package com.hadoop.compression.lzo.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;


/**
 * Provides a compatibility layer for handling API incompatibilities between
 * hadoop 1.x and 2.x via reflection.
 */
public class CompatibilityUtil {
  private static final boolean useV2;

  private static final Constructor<?> TASK_ATTEMPT_CONTEXT_CONSTRUCTOR;

  private static final Method GET_CONFIGURATION;

  static {
    boolean v2 = true;
    try {
      // use the presence of JobContextImpl as a test for 2.x
      Class.forName("org.apache.hadoop.mapreduce.task.JobContextImpl");
    } catch (ClassNotFoundException cnfe) {
      v2 = false;
    }
    useV2 = v2;

    try {
      Class<?> taskAttemptContextCls =
        Class.forName(useV2 ?
          "org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl" :
          "org.apache.hadoop.mapreduce.TaskAttemptContext");
      TASK_ATTEMPT_CONTEXT_CONSTRUCTOR =
        taskAttemptContextCls.getConstructor(Configuration.class,
                                             TaskAttemptID.class);

      GET_CONFIGURATION =
        Class.forName("org.apache.hadoop.mapreduce.JobContext")
          .getMethod("getConfiguration");
    } catch (SecurityException e) {
      throw new IllegalArgumentException("Can't run constructor ", e);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException("Can't find constructor ", e);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Can't find class", e);
    }
  }

  /**
   * Returns true whether the runtime hadoop version is 2.x, false otherwise.
   */
  public static boolean isVersion2x() {
    return useV2;
  }

  private static Object newInstance(Constructor<?> constructor, Object... args) {
    try {
      return constructor.newInstance(args);
    } catch (InstantiationException e) {
      throw new IllegalArgumentException("Can't instantiate " + constructor, e);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Can't instantiate " + constructor, e);
    } catch (InvocationTargetException e) {
      throw new IllegalArgumentException("Can't instantiate " + constructor, e);
    }
  }

  /**
   * Creates TaskAttempContext from a JobConf and jobId using the correct
   * constructor for based on the hadoop version.
   */
  public static TaskAttemptContext newTaskAttemptContext(Configuration conf,
                                                         TaskAttemptID id) {
    return (TaskAttemptContext)
        newInstance(TASK_ATTEMPT_CONTEXT_CONSTRUCTOR, conf, id);
  }

  private static Object invoke(Method method, Object obj, Object... args) {
    try {
      return method.invoke(obj, args);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Can't invoke method " + method.getName(), e);
    } catch (InvocationTargetException e) {
      throw new IllegalArgumentException("Can't invoke method " + method.getName(), e);
    }
  }

  /**
   * Invokes getConfiguration() on JobContext. Works with both
   * hadoop 1 and 2.
   */
  public static Configuration getConfiguration(JobContext context) {
    return (Configuration)invoke(GET_CONFIGURATION, context);
  }
}