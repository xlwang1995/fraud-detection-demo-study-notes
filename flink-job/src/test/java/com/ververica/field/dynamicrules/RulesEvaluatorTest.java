/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.field.dynamicrules;

import com.ververica.field.dynamicrules.RulesEvaluator.Descriptors;
import com.ververica.field.dynamicrules.functions.DynamicAlertFunction;
import com.ververica.field.dynamicrules.functions.DynamicKeyFunction;
import com.ververica.field.dynamicrules.util.AssertUtils;
import com.ververica.field.dynamicrules.util.BroadcastStreamKeyedOperatorTestHarness;
import com.ververica.field.dynamicrules.util.BroadcastStreamNonKeyedOperatorTestHarness;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.junit.Test;

/** Tests for the {@link RulesEvaluator}. */
public class RulesEvaluatorTest {

  // 先测试keyBy之后的数据
  @Test
  public void shouldProduceKeyedOutput() throws Exception {
    RuleParser ruleParser = new RuleParser();
    Rule rule1 =
        ruleParser.fromString("1,(active),(paymentType&payeeId),,(totalFare),(SUM),(>),(50),(20)");
    Transaction event1 = Transaction.fromString("1,2013-01-01 00:00:00,1001,1002,CSH,21.5,1");

    // 利用辅助类 BroadcastStreamNonKeyedOperatorTestHarness 的 getInitializedTestHarness 函数，
    // 构造一个原始数据-规则-keyed结构的数据
 try (BroadcastStreamNonKeyedOperatorTestHarness<
            Transaction, Rule, Keyed<Transaction, String, Integer>>
        testHarness =
            BroadcastStreamNonKeyedOperatorTestHarness.getInitializedTestHarness(
                new DynamicKeyFunction(), Descriptors.rulesDescriptor)) {

      // 测试 processElement2、processElement1 功能
      testHarness.processElement2(new StreamRecord<>(rule1, 12L));
      testHarness.processElement2(new StreamRecord<>(event1, 15L));

      // 想要的结果以队列形式呈现
      // ConcurrentLinkedQueue 是一个基于链接节点的无界线程安全队列
      Queue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
      expectedOutput.add(
          new StreamRecord<>(new Keyed<>(event1, "{paymentType=CSH;payeeId=1001}", 1), 15L));

      // 使用了flink自带的测试类
      // /**
      //  * Compare the two queues containing operator/task output by converting them to an array first.
      //  */
      // public static <T> void assertOutputEquals(String message, Queue<T> expected, Queue<T> actual) {
      //     Assert.assertArrayEquals(message, expected.toArray(), actual.toArray());
      // }
      // 比较两个队列结果

      TestHarnessUtil.assertOutputEquals(
          "Wrong dynamically keyed output", expectedOutput, testHarness.getOutput());
    }
  }

  // 测试规则状态保存是否正确
  @Test
  public void shouldStoreRulesInBroadcastStateDuringDynamicKeying() throws Exception {
    RuleParser ruleParser = new RuleParser();
    Rule rule1 = ruleParser.fromString("1,(active),(paymentType),,(totalFare),(SUM),(>),(50),(20)");

    try (BroadcastStreamNonKeyedOperatorTestHarness<
            Transaction, Rule, Keyed<Transaction, String, Integer>>
        testHarness =
            BroadcastStreamNonKeyedOperatorTestHarness.getInitializedTestHarness(
                new DynamicKeyFunction(), Descriptors.rulesDescriptor)) {

      testHarness.processElement2(new StreamRecord<>(rule1, 12L));

      BroadcastState<Integer, Rule> broadcastState =
          testHarness.getBroadcastState(Descriptors.rulesDescriptor);

      Map<Integer, Rule> expectedState = new HashMap<>();
      expectedState.put(rule1.getRuleId(), rule1);

      AssertUtils.assertEquals(broadcastState, expectedState, "Output was not correct.");
    }
  }


  // 测试flink侧边流输出
  @Test
  public void shouldOutputSimplestAlert() throws Exception {
    RuleParser ruleParser = new RuleParser();
    Rule rule1 =
        ruleParser.fromString("1,(active),(paymentType),,(paymentAmount),(SUM),(>),(20),(20)");

    Transaction event1 = Transaction.fromString("1,2013-01-01 00:00:00,1001,1002,CSH,22,1");
    Transaction event2 = Transaction.fromString("2,2013-01-01 00:00:01,1001,1002,CRD,19,1");
    Transaction event3 = Transaction.fromString("3,2013-01-01 00:00:02,1001,1002,CRD,2,1");

    Keyed<Transaction, String, Integer> keyed1 = new Keyed<>(event1, "CSH", 1);
    Keyed<Transaction, String, Integer> keyed2 = new Keyed<>(event2, "CRD", 1);
    Keyed<Transaction, String, Integer> keyed3 = new Keyed<>(event3, "CRD", 1);

    try (BroadcastStreamKeyedOperatorTestHarness<
            String, Keyed<Transaction, String, Integer>, Rule, Alert>
        testHarness =
            BroadcastStreamKeyedOperatorTestHarness.getInitializedTestHarness(
                new DynamicAlertFunction(),
                in -> (in.getKey()),
                null,
                BasicTypeInfo.STRING_TYPE_INFO,
                Descriptors.rulesDescriptor)) {

      testHarness.processElement2(new StreamRecord<>(rule1, 12L));

      testHarness.processElement1(new StreamRecord<>(keyed1, 15L));
      testHarness.processElement1(new StreamRecord<>(keyed2, 16L));
      testHarness.processElement1(new StreamRecord<>(keyed3, 17L));

      ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
      Alert<Transaction, BigDecimal> alert1 =
          new Alert<>(rule1.getRuleId(), rule1, "CSH", event1, BigDecimal.valueOf(22));
      Alert<Transaction, BigDecimal> alert2 =
          new Alert<>(rule1.getRuleId(), rule1, "CRD", event3, BigDecimal.valueOf(21));

      expectedOutput.add(new StreamRecord<>(alert1, 15L));
      expectedOutput.add(new StreamRecord<>(alert2, 17L));

      TestHarnessUtil.assertOutputEquals(
          "Output was not correct.", expectedOutput, testHarness.getOutput());
    }
  }

  // 测试 "是否正确处理拥有相同时间戳的原始数据"
  @Test
  public void shouldHandleSameTimestampEventsCorrectly() throws Exception {
    RuleParser ruleParser = new RuleParser();
    Rule rule1 =
        ruleParser.fromString("1,(active),(paymentType),,(paymentAmount),(SUM),(>),(20),(20)");

    Transaction event1 = Transaction.fromString("1,2013-01-01 00:00:00,1001,1002,CSH,19,1");

    Transaction event2 = Transaction.fromString("2,2013-01-01 00:00:00,1002,1003,CSH,2,1");

    Keyed<Transaction, String, Integer> keyed1 = new Keyed<>(event1, "CSH", 1);
    Keyed<Transaction, String, Integer> keyed2 = new Keyed<>(event2, "CSH", 1);

    try (BroadcastStreamKeyedOperatorTestHarness<
            String, Keyed<Transaction, String, Integer>, Rule, Alert>
        testHarness =
            BroadcastStreamKeyedOperatorTestHarness.getInitializedTestHarness(
                new DynamicAlertFunction(),
                in -> (in.getKey()),
                null,
                BasicTypeInfo.STRING_TYPE_INFO,
                Descriptors.rulesDescriptor)) {

      testHarness.processElement2(new StreamRecord<>(rule1, 12L));

      testHarness.processElement1(new StreamRecord<>(keyed1, 15L));
      testHarness.processElement1(new StreamRecord<>(keyed2, 16L));

      ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
      Alert<Transaction, BigDecimal> alert1 =
          new Alert<>(rule1.getRuleId(), rule1, "CSH", event2, new BigDecimal(21));

      expectedOutput.add(new StreamRecord<>(alert1, 16L));

      TestHarnessUtil.assertOutputEquals(
          "Output was not correct.", expectedOutput, testHarness.getOutput());
    }
  }
  // 测试 测试是否正确基于水印清除状态
  @Test
  public void shouldCleanupStateBasedOnWatermarks() throws Exception {
    RuleParser ruleParser = new RuleParser();
    Rule rule1 =
        ruleParser.fromString("1,(active),(paymentType),,(paymentAmount),(SUM),(>),(10),(4)");

    Transaction event1 = Transaction.fromString("1,2013-01-01 00:01:00,1001,1002,CSH,3,1");

    Transaction event2 = Transaction.fromString("2,2013-01-01 00:02:00,1003,1004,CSH,3,1");

    Transaction event3 = Transaction.fromString("3,2013-01-01 00:03:00,1005,1006,CSH,5,1");

    Transaction event4 = Transaction.fromString("4,2013-01-01 00:06:00,1007,1008,CSH,3,1");

    Keyed<Transaction, String, Integer> keyed1 = new Keyed<>(event1, "CSH", 1);
    Keyed<Transaction, String, Integer> keyed2 = new Keyed<>(event2, "CSH", 1);
    Keyed<Transaction, String, Integer> keyed3 = new Keyed<>(event3, "CSH", 1);
    Keyed<Transaction, String, Integer> keyed4 = new Keyed<>(event4, "CSH", 1);

    try (BroadcastStreamKeyedOperatorTestHarness<
            String, Keyed<Transaction, String, Integer>, Rule, Alert>
        testHarness =
            BroadcastStreamKeyedOperatorTestHarness.getInitializedTestHarness(
                new DynamicAlertFunction(),
                in -> (in.getKey()),
                null,
                BasicTypeInfo.STRING_TYPE_INFO,
                Descriptors.rulesDescriptor)) {

      //      long halfAMinuteMillis = 30 * 1000l;
      long watermarkDelay = 2 * 60 * 1000l;

      testHarness.processElement2(new StreamRecord<>(rule1, 1L));

      testHarness.processElement1(toStreamRecord(keyed1));
      testHarness.watermark(event1.getEventTime() - watermarkDelay);

      testHarness.processElement1(toStreamRecord(keyed2));
      testHarness.watermark(event2.getEventTime() - watermarkDelay);

      testHarness.processElement1(toStreamRecord(keyed4));
      testHarness.watermark(event4.getEventTime() - watermarkDelay);

      // Cleaning up on per-event fixed basis had caused event4 to delete event1 from the state,
      // hence event3 would not have fired. We expect event3 to fire.
      // 在每个事件固定的基础上进行清理导致 event4 从状态中删除 event1，
      // 因此 event3 不会被解雇。 我们期望 event3 触发。
      testHarness.processElement1(toStreamRecord(keyed3));

      ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
      Alert<Transaction, BigDecimal> alert1 =
          new Alert<>(rule1.getRuleId(), rule1, "CSH", event3, new BigDecimal(11));

      expectedOutput.add(new StreamRecord<>(alert1, event3.getEventTime()));

      TestHarnessUtil.assertOutputEquals(
          "Output was not correct.", expectedOutput, filterOutWatermarks(testHarness.getOutput()));
    }
  }

  private StreamRecord<Keyed<Transaction, String, Integer>> toStreamRecord(
      Keyed<Transaction, String, Integer> keyed) {
    return new StreamRecord<>(keyed, keyed.getWrapped().getEventTime());
  }

  private Queue<Object> filterOutWatermarks(Queue<Object> in) {
    Queue<Object> out = new LinkedList<>();
    for (Object o : in) {
      if (!(o instanceof Watermark)) {
        out.add(o);
      }
    }
    return out;
  }
}
