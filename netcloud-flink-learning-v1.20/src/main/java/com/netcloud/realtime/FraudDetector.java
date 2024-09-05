package com.netcloud.realtime;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

/**
 * 根据账户分组后，核心处理逻辑
 */
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final long serialVersionUID = 1L;

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;
    private transient ValueState<Boolean> flagState;
    private transient ValueState<Long> timerState;


    @Override
    public void open(OpenContext openContext) {
        //初始化 flagState 和 timerState。flagState 用于存储标志状态，timerState 用于存储定时器的时间戳。
        ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
                "flag",
                Types.BOOLEAN);
        flagState = getRuntimeContext().getState(flagDescriptor);

        ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>(
                "timer-state",
                Types.LONG);
        timerState = getRuntimeContext().getState(timerDescriptor);
    }

    @Override
    public void processElement(
            Transaction transaction,
            Context context,
            Collector<Alert> collector) throws Exception {

        //获取状态值
        Boolean lastTransactionWasSmall = flagState.value();

        // 判断状态是否被设置(上一条数据为小金额交易)
        if (lastTransactionWasSmall != null) {
            //目前交易是大额交易(>500)
            if (transaction.getAmount() > LARGE_AMOUNT) {
                //Output an alert downstream
                Alert alert = new Alert();
                alert.setId(transaction.getAccountId());
                collector.collect(alert);
            }
            //清除所有的状态(上一笔交易是小额交易但是当前非大额交易)
            cleanUp(context);
        }

        //如果目前交易是小额交易,保存状态后续使用
        if (transaction.getAmount() < SMALL_AMOUNT) {
            //保存状态
            flagState.update(true);

            //设置1分钟定时器
            long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
            context.timerService().registerProcessingTimeTimer(timer);
            timerState.update(timer);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) {
        // 当定时器触发时，重置标志状态。
        timerState.clear();
        flagState.clear();
    }

    private void cleanUp(Context ctx) throws Exception {
        // 删除定时器
        Long timer = timerState.value();
        ctx.timerService().deleteProcessingTimeTimer(timer);

        // 清除所有状态
        timerState.clear();
        flagState.clear();
    }
}
