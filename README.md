## 电商实时数仓项目

**项目描述**：基于 Docker 搭建 Flink + Kafka + Doris 实时数仓生态集群，模拟电商用户行为数据（浏览/点击/加购/支付/退款）的实时采集、清洗、聚合与可视化展示，为运营决策提供分钟级数据支撑。

**技术栈**：Flink 1.19.1、Kafka 3.x、Doris 3.0.4、Redis 7.0、MySQL 8.0、Zookeeper 3.5.7、Superset、Docker

**项目职责**：
1. **数仓分层建模**：设计 ODS→DWD→DWS→ADS 四层实时数仓架构，ODS 层通过 Kafka 采集 4 路业务数据流，DWD 层完成双流 join 关联与状态去重，DWS 层按商品/时间维度聚合 GMV/客单价/转化率/漏斗指标，ADS 层落地 4 个实时指标作业（1min GMV、商品排行、商品漏斗、运营总览）。
2. **Flink 实时开发**：基于 EventTime 语义实现 8+ 个 Flink 作业，采用 TumblingEventTimeWindows + allowedLateness 处理乱序数据，使用 KeyedCoProcessFunction + ValueState 实现双流 join，解决漏斗流数据稀疏导致的窗口不匹配问题（GMV 驱动输出模式）。
3. **维度关联与状态管理**：Redis 缓存商品维度表，异步 IO 关联实现毫秒级维度补全；设计订单状态机（CREATED→PAID→SHIPPED→REFUNDED）与注册用户池/活跃商品池，保证多流数据可关联性。
4. **Doris 实时入仓与调优**：通过 Routine Load 实现 Kafka→Doris 秒级数据同步，针对 BE 启动失败、tablet 副本异常、网络隔离等问题进行调优，最终将 BUCKETS 从 3 降至 1、内存限制从 2g 优化至 1g，在资源受限环境下稳定运行。
5. **可视化看板**：配置 Superset 连接 Doris，搭建 GMV 趋势、商品排行、转化漏斗、运营总览 4 张实时 Dashboard。

**性能优化**：
1. **生成器优化**：设计 80% 已注册用户复用率 + 70% 活跃商品复用率，使 DWD 层双流 join 成功率从随机生成的 <5% 提升至 80%+。
2. **窗口对齐优化**：将 ADS 层双流连窗 join 从"双向匹配"改为"GMV 流驱动输出"，解决漏斗流稀疏导致的输出缺失问题，数据输出率从 <1% 提升至 100%。
