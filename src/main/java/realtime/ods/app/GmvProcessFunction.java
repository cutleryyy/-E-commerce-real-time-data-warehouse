package realtime.ods.app;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import com.test.UserBehavior;

public class GmvProcessFunction extends KeyedProcessFunction<String, UserBehavior,Double> {
//    定义统计变量
    private ValueState<Long> gmvState;

    @Override
    public void open(Configuration parameters) throws Exception {

        ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>("gmv-state", Long.class);
        gmvState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(UserBehavior value,
                               KeyedProcessFunction<String, UserBehavior, Double>.Context ctx,
                               Collector<Double> out) throws Exception {
//       获取当前值
        Long current = gmvState.value();
//       如果是初始值null
        if (current == null){
            current =0l;
        }
//      重写processElement方法时，传入UserBehavior对象并命名为value
        current += value.getAmount();
//       每次计算后都更新gmv为当前计算累加值
        gmvState.update(current);
//       collect收集当前最新结果
//        out.collect(current);
    }
}
