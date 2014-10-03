package com.hadoop.compression.lzo.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;


/**
 * Provides a compatibility layer for handling API incompatibilities between
 * hadoop 1.x and 2.x via reflection.
 */
public class CompatibilityUtil {
  private static final boolean useV2;

  private static final Constructor<?> TASK_ATTEMPT_CONTEXT_CONSTRUCTOR;

  private static final Method GET_CONFIGURATION;
  private static final Method GET_COUNTER_ENUM_METHOD;
  private static final Method INCREMENT_COUNTER_METHOD;
  private static final Method GET_COUNTER_VALUE_METHOD;

  private static final String PACKAGE = "org.apache.hadoop.mapreduce";

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
          PACKAGE + ".task.TaskAttemptContextImpl" :
          PACKAGE + ".TaskAttemptContext");
      TASK_ATTEMPT_CONTEXT_CONSTRUCTOR =
        taskAttemptContextCls.getConstructor(Configuration.class,
                                             TaskAttemptID.class);

      GET_CONFIGURATION = getMethod(".JobContext", "getConfiguration");
      INCREMENT_COUNTER_METHOD = getMethod(".Counter", "increment", Long.TYPE);
      GET_COUNTER_VALUE_METHOD = getMethod(".Counter", "getValue");

      if (useV2) {
        Method get_counter;
        try {
          get_counter = getMethod(".TaskAttemptContext", "getCounter", Enum.class);
        } catch (Exception e) {
          get_counter = getMethod(".TaskInputOutputContext", "getCounter", Enum.class);
        }
        GET_COUNTER_ENUM_METHOD = get_counter;
      } else {
        GET_COUNTER_ENUM_METHOD = getMethod(".TaskInputOutputContext", "getCounter", Enum.class);
      }

    } catch (SecurityException e) {
      throw new IllegalArgumentException("Can't run constructor ", e);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException("Can't find constructor ", e);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Can't find class", e);
    }
  }

  /**
   * Wrapper for Class.forName(className).getMethod(methodName, parmeterTypes);
   */
  private static Method getMethod(String className, String methodName, Class<?>... paramTypes)
      throws ClassNotFoundException, NoSuchMethodException {
    return Class.forName(className.startsWith(".") ? PACKAGE + className : className)
        .getMethod(methodName, paramTypes);
  }

  /**
   * Returns true whether the runtime hadoop version is 2.x, false otherwise.
   * @return true or false
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
   * @param conf {@link Configuration}
   * @param id {@link TaskAttemptID}
   * @return {@link TaskAttemptContext}
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
   * @param context {@link JobContext}
   * @return {@link Configuration}
   */
  public static Configuration getConfiguration(JobContext context) {
    return (Configuration)invoke(GET_CONFIGURATION, context);
  }

  /**
   * Invoke getCounter() on TaskInputOutputContext. Works with both
   * Hadoop 1 and 2.
   * @param context {@link TaskInputOutputContext}
   * @param counter Mapreduce counter
   * @return {@link Counter}
   */
  public static Counter getCounter(TaskInputOutputContext context, Enum<?> counter) {
    return (Counter) invoke(GET_COUNTER_ENUM_METHOD, context, counter);
  }

  /**
   * Increment the counter. Works with both Hadoop 1 and 2
   * @param counter {@link Counter}
   * @param increment Increment this counter by the given value
   */
  public static void incrementCounter(Counter counter, long increment) {
    invoke(INCREMENT_COUNTER_METHOD, counter, increment);
  }

  /**
   * Hadoop 1 and 2 compatible counter.getValue()
   * @param counter {@link Counter}
   * @return {@link Counter#getValue()}
   */
  public static long getCounterValue(Counter counter) {
    return (Long) invoke(GET_COUNTER_VALUE_METHOD, counter);
  }
}