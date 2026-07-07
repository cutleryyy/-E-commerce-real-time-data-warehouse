package test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.storage.StorageLevel;

import static org.apache.spark.sql.functions.*;
public class hive_dws {
    public static void main(String[] args) throws InterruptedException {

        SparkSession spark = SparkSession.builder()
                .appName("hive_dwd")
//                .master("spark://192.168.94.130:7077")
//                .config("spark.ui.port", "4040")
//                config("spark.driver.host","172.31.62.14")
                .master("local[*]")
//                .config("spark.executor.instances", "1")   // 因为只有一个 Worker
//                .config("spark.executor.cores", "2")
//                .config("spark.cores.max", "2")// 核数为cpu核的2-3倍
                .enableHiveSupport().getOrCreate();
//      开启AQE：自动调整shuffle分区、自动优化join、缓解数据倾斜
        spark.conf().set(
                "spark.sql.adaptive.enabled",
                "true"
        );
        spark.conf().set("spark.sql.adaptive.enabled", "true");

        spark.conf().set("spark.sql.dialect", "hive");
        spark.conf().set("spark.sql.shuffle.partitions", "10");
        spark.conf().set("spark.sql.adaptive.coalescePartitions.enabled", "true");
        // 启用 Hive 动态分区支持,关闭严格模式
        spark.conf().set("hive.exec.dynamic.partition", "true");
        spark.conf().set("hive.exec.dynamic.partition.mode", "nonstrict");
        // 优化动态分区写入性能
        spark.conf().set("spark.sql.sources.partitionOverwriteMode", "dynamic");
        spark.conf().set("spark.sql.adaptive.skewJoin.enabled", "true");

//        Thread.sleep(600000);
        final long start = System.currentTimeMillis();
///*
//        ========================= dws_patient===================
        spark.sql("drop table if exists medical_spark.dws_patient_clinic_summary");
spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS medical_spark.dws_patient_clinic_summary (\n" +
        "    patient_id BIGINT COMMENT '患者ID',\n" +
        "    patient_name STRING COMMENT '患者姓名',\n" +
        "    patient_gender STRING COMMENT '性别',\n" +
        "    patient_age INT COMMENT '年龄',\n" +
        "    age_group STRING COMMENT '年龄段',\n" +
        "    total_visit_count INT COMMENT '总就诊次数',\n" +
        "    total_fee DECIMAL(10,2) COMMENT '总费用',\n" +
        "    avg_fee_per_visit DECIMAL(10,2) COMMENT '平均每次就诊费用',\n" +
        "    max_fee DECIMAL(10,2) COMMENT '最高就诊费用',\n" +
        "    min_fee DECIMAL(10,2) COMMENT '最低就诊费用',\n" +
        "    frequent_dept_id BIGINT COMMENT '就诊最频繁科室ID',\n" +
        "    frequent_dept_name STRING COMMENT '就诊最频繁科室名称',\n" +
        "    frequent_doctor_id STRING COMMENT '就诊最频繁医生ID',\n" +
        "    frequent_doctor_name STRING COMMENT '就诊最频繁医生姓名',\n" +
        "    first_visit_date DATE COMMENT '首次就诊日期',\n" +
        "    last_visit_date DATE COMMENT '最近就诊日期',\n" +
        "    visit_days_count INT COMMENT '就诊天数',\n" +
        "    avg_visit_interval INT COMMENT '平均就诊间隔（天）',\n" +
        "    main_diagnosis STRING COMMENT '主要诊断',\n" +
        "    total_prescription_count INT COMMENT '总处方数',\n" +
        "    total_medicine_count INT COMMENT '总药品数'\n" +
        ")\n" +
        "STORED AS ORC\n" +
        "LOCATION '/user/hive/warehouse/medical_spark.db/dws_patient_clinic_summary'\n" +
        "TBLPROPERTIES (\n" +
        "    'orc.compress'='SNAPPY',\n" +
        "    'comment'='患者就诊主题汇总表'\n" +
        ")");

//        Dataset<Row> clinic = spark.table("medical_spark.dwd_clinic");
//        对于反复需要读取的表，通过持久化策略缓存在内存中，以提升读取速度，不，小数据量这样磁盘IO更慢
        Dataset<Row> clinic =spark.table("medical_spark.dwd_clinic").persist(StorageLevel.MEMORY_ONLY());

        Dataset<Row> prescription = spark.table("medical_spark.dwd_prescription");
        Dataset<Row> base = clinic
                .groupBy("patient_id", "patient_name", "patient_gender","patient_age")
                .agg(
                        count("*").alias("total_visit_count"),
                        round(sum("fee"),2).alias("total_fee"),
                        round(avg("fee"),2).alias("avg_fee_per_visit"),
                        round(max("fee"),2).alias("max_fee"),
                        round(min("fee"),2).alias("min_fee"),
                        min(to_date(col("visit_date"))).alias("first_visit_date"),
                        max(to_date(col("visit_date"))).alias("last_visit_date"),
                        countDistinct("visit_date").alias("visit_days_count")
                )
                .withColumn("age_group",
                        when(col("patient_age").lt(18), "0-17岁")
                                .when(col("patient_age").lt(30), "18-29岁")
                                .when(col("patient_age").lt(45), "30-44岁")
                                .when(col("patient_age").lt(60), "45-59岁")
                                .otherwise("60岁及以上")
                )
                .withColumn("avg_visit_interval",
                        when(
                                col("visit_days_count").gt(1),
                                round(date_diff(col("last_visit_date"),col("first_visit_date"))
                                        .divide(col("visit_days_count").minus(1)),0).cast("int")
                                        ).otherwise(0)
                );
//      取每位患者的最频繁就诊科室
//        Dataset<Row> dept_rank = clinic
//                .groupBy("patient_id","dept_id","dept_name")
//                .agg(count("*").alias("cnt")
//                ,max(struct(col("cnt"), col("dept_id"), col("dept_name"))).alias("max_struct"))
//                .withColumn("dept_rank",row_number().over(Window.partitionBy("patient_id").orderBy(col("cnt").desc())))
//                .filter(col("dept_rank").equalTo(1));
        Dataset<Row> deptCounts = clinic
                .groupBy("patient_id", "dept_id", "dept_name")
                .agg(count("*").alias("cnt"));

        Dataset<Row> deptRank = deptCounts
                .groupBy("patient_id")
                .agg(max(struct(col("cnt"), col("dept_id"), col("dept_name"))).alias("max_struct"))
                .select(
                        col("patient_id"),
                        col("max_struct.dept_id").alias("frequent_dept_id"),
                        col("max_struct.dept_name").alias("frequent_dept_name")
                );

//      按患者+医生统计就诊次数，取最频繁医生
        Dataset<Row> doctor_rank = clinic.groupBy("patient_id","doctor_id","doctor_name")
                .agg(count(col("*")).alias("cnt"))
                .withColumn("doctor_rank",row_number().over(Window.partitionBy("patient_id").orderBy(col("cnt").desc())))
                .filter(col("doctor_rank").equalTo(1));
//      按患者+诊断统计次数，取主要诊断
        Dataset<Row> diagnosis_rank = clinic.groupBy("patient_id","diagnosis")
                .agg(count(col("*")).alias("cnt"))
                .withColumn("diagnosis_rank",row_number().over(Window.partitionBy("patient_id").orderBy(col("cnt").desc())))
                .filter(col("diagnosis_rank").equalTo(1));
//      汇总每个患者的开方数量及药品数量
        Dataset<Row> prescription_sum = prescription.groupBy("patient_id")
                        .agg(count_distinct(col("id")).alias("total_prescription_count"),sum(col("quantity")).alias("total_medicine_count"));
//
//        Dataset<Row> result = base.join(broadcast(dept_rank),base.col("patient_id").equalTo(dept_rank.col("patient_id")),"left")
//                .join(broadcast(doctor_rank),base.col("patient_id").equalTo(doctor_rank.col("patient_id")),"left").drop(doctor_rank.col("patient_id"))
//                .join(broadcast(diagnosis_rank),base.col("patient_id").equalTo(diagnosis_rank.col("patient_id")),"left").drop(doctor_rank.col("patient_id"))
//                .join(broadcast(prescription_sum),base.col("patient_id").equalTo(prescription_sum.col("patient_id")),"left").drop(doctor_rank.col("patient_id"))
//                .select(base.col("patient_id"),base.col("patient_name"),base.col("patient_gender"),
//                        base.col("patient_age"),base.col("age_group"),base.col("total_visit_count"),
//                        base.col("total_fee"),base.col("avg_fee_per_visit"),base.col("max_fee"),base.col("min_fee"),
//                        dept_rank.col("dept_id").alias("frequent_dept_id"),dept_rank.col("dept_name").alias("frequent_dept_name"),
//                        doctor_rank.col("doctor_id").alias("frequent_doctor_id"),doctor_rank.col("doctor_name").alias("frequent_doctor_name"),
//                        base.col("first_visit_date"),base.col("last_visit_date"),base.col("visit_days_count"),base.col("avg_visit_interval"),
//                        diagnosis_rank.col("diagnosis").alias("main_diagnosis"),
//                        prescription_sum.col("total_prescription_count"),prescription_sum.col("total_medicine_count")
//                );

        Dataset<Row> b = base.as("b");
        Dataset<Row> d1 = deptRank.as("d1");
        Dataset<Row> d2 = doctor_rank.as("d2");
        Dataset<Row> d3 = diagnosis_rank.as("d3");
        Dataset<Row> d4 = prescription_sum.as("d4");

        Dataset<Row> result =
                b.join(
                                broadcast(d1),
                                col("b.patient_id").equalTo(col("d1.patient_id")),
                                "left"
                        )
                        .join(
                                broadcast(d2),
                                col("b.patient_id").equalTo(col("d2.patient_id")),
                                "left"
                        )
                        .join(
                                broadcast(d3),
                                col("b.patient_id").equalTo(col("d3.patient_id")),
                                "left"
                        )
                        .join(
                                broadcast(d4),
                                col("b.patient_id").equalTo(col("d4.patient_id")),
                                "left"
                        )
                        .select(
                col("b.patient_id"),
                col("b.patient_name"),
                col("b.patient_gender"),col("b.patient_age"),col("b.age_group"),
                col("b.total_visit_count"),col("b.total_fee"),
                col("b.avg_fee_per_visit"),col("b.max_fee"),col("b.min_fee"),
                col("d1.frequent_dept_id"),
                col("d1.frequent_dept_name"),
                col("d2.doctor_id").alias("frequent_doctor_id"),
                col("d2.doctor_name").alias("frequent_doctor_name"),
                col("b.first_visit_date"),col("b.last_visit_date"),col("b.visit_days_count"),col("b.avg_visit_interval"),
                col("d3.diagnosis").alias("main_diagnosis"),
                col("d4.total_prescription_count"),
                col("d4.total_medicine_count")
        );


//        result.explain(true);
//        base.printSchema();
//        dept_rank.printSchema();
//        doctor_rank.printSchema();
//        diagnosis_rank.printSchema();

        result.write().mode(SaveMode.Overwrite).insertInto("medical_spark.dws_patient_clinic_summary");
        final long end = System.currentTimeMillis();
        long time = end - start;

        System.out.println("执行时间为：" + time + "ms");

// */
//        spark.sql("select * from medical_spark.dws_patient_clinic_summary limit 10").show();
//        spark.sql("select count(*) from medical_spark.dws_patient_clinic_summary").show();
//        spark.sql("select count(*) from medical_spark.dws_patient_clinic_summary where total_prescription_count is null").show();
   }

}
/*
    spark.sql("INSERT OVERWRITE TABLE medical_dws.dws_patient_clinic_summary PARTITION(data_date='2026-03-19')\n" +
            "-- 第一步：基础就诊汇总（按患者）\n" +
            "WITH base AS (\n" +
            "    SELECT\n" +
            "        patient_id,\n" +
            "        MAX(patient_name) AS patient_name,\n" +
            "        MAX(patient_gender) AS gender,\n" +
            "        MAX(patient_age) AS age,\n" +
            "        COUNT(*) AS total_visit_count,\n" +
            "        SUM(fee) AS total_fee,\n" +
            "        AVG(fee) AS avg_fee_per_visit,\n" +
            "        MAX(fee) AS max_fee,\n" +
            "        MIN(fee) AS min_fee,\n" +
            "        MIN(visit_date) AS first_visit_date,\n" +
            "        MAX(visit_date) AS last_visit_date,\n" +
            "        COUNT(DISTINCT visit_date) AS visit_days_count\n" +
            "    FROM medical_dwd.dwd_clinic\n" +
            "    GROUP BY patient_id\n" +
            "),\n" +
            "\n" +
            "-- 第二步：计算每个患者的年龄分组\n" +
            "age_grouped AS (\n" +
            "    SELECT\n" +
            "        patient_id,\n" +
            "        CASE\n" +
            "            WHEN age < 18 THEN '0-17岁'\n" +
            "            WHEN age < 30 THEN '18-29岁'\n" +
            "            WHEN age < 45 THEN '30-44岁'\n" +
            "            WHEN age < 60 THEN '45-59岁'\n" +
            "            ELSE '60岁以上'\n" +
            "        END AS age_group\n" +
            "    FROM base\n" +
            "),\n" +
            "\n" +
            "-- 第三步：按患者+科室统计就诊次数，取最频繁科室\n" +
            "dept_rank AS (\n" +
            "    SELECT\n" +
            "        patient_id,\n" +
            "        dept_id,\n" +
            "        dept_name,\n" +
            "        COUNT(*) AS cnt,\n" +
            "        ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY COUNT(*) DESC) AS rn\n" +
            "    FROM medical_dwd.dwd_clinic\n" +
            "    WHERE data_date = '2026-03-19'\n" +
            "    GROUP BY patient_id, dept_id, dept_name\n" +
            "),\n" +
            "frequent_dept AS (\n" +
            "    SELECT\n" +
            "        patient_id,\n" +
            "        dept_id AS frequent_dept_id,\n" +
            "        dept_name AS frequent_dept_name\n" +
            "    FROM dept_rank\n" +
            "    WHERE rn = 1\n" +
            "),\n" +
            "\n" +
            "-- 第四步：按患者+医生统计就诊次数，取最频繁医生\n" +
            "doctor_rank AS (\n" +
            "    SELECT\n" +
            "        patient_id,\n" +
            "        doctor_id,\n" +
            "        doctor_name,\n" +
            "        COUNT(*) AS cnt,\n" +
            "        ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY COUNT(*) DESC) AS rn\n" +
            "    FROM medical_dwd.dwd_clinic\n" +
            "    WHERE data_date = '2026-03-19'\n" +
            "    GROUP BY patient_id, doctor_id, doctor_name\n" +
            "),\n" +
            "frequent_doctor AS (\n" +
            "    SELECT\n" +
            "        patient_id,\n" +
            "        doctor_id AS frequent_doctor_id,\n" +
            "        doctor_name AS frequent_doctor_name\n" +
            "    FROM doctor_rank\n" +
            "    WHERE rn = 1\n" +
            "),\n" +
            "\n" +
            "-- 第五步：按患者+诊断统计次数，取主要诊断\n" +
            "diagnose_rank AS (\n" +
            "    SELECT\n" +
            "        patient_id,\n" +
            "        diagnosis,\n" +
            "        COUNT(*) AS cnt,\n" +
            "        ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY COUNT(*) DESC) AS rn\n" +
            "    FROM medical_dwd.dwd_clinic\n" +
            "    WHERE data_date = '2026-03-19'\n" +
            "    GROUP BY patient_id, diagnosis\n" +
            "),\n" +
            "main_diagnose AS (\n" +
            "    SELECT\n" +
            "        patient_id,\n" +
            "        diagnosis AS main_diagnose\n" +
            "    FROM diagnose_rank\n" +
            "    WHERE rn = 1\n" +
            "),\n" +
            "\n" +
            "-- 第六步：处方相关汇总（按患者）\n" +
            "prescription_sum AS (\n" +
            "    SELECT\n" +
            "        p.patient_id,\n" +
            "        COUNT(DISTINCT p.id) AS total_prescription_count,\n" +
            "        SUM(p.quantity) AS total_medicine_count\n" +
            "    FROM medical_dwd.dwd_prescription p\n" +
            "    WHERE p.data_date = '2026-03-19'\n" +
            "    GROUP BY p.patient_id\n" +
            ")\n" +
            "-- 最终合并\n" +
            "SELECT\n" +
            "    b.patient_id,\n" +
            "    b.patient_name,\n" +
            "    b.gender,\n" +
            "    b.age,\n" +
            "    ag.age_group,\n" +
            "    b.total_visit_count,\n" +
            "    b.total_fee,\n" +
            "    b.avg_fee_per_visit,\n" +
            "    b.max_fee,\n" +
            "    b.min_fee,\n" +
            "    fd.frequent_dept_id,\n" +
            "    fd.frequent_dept_name,\n" +
            "    fdoc.frequent_doctor_id,\n" +
            "    fdoc.frequent_doctor_name,\n" +
            "    b.first_visit_date,\n" +
            "    b.last_visit_date,\n" +
            "    b.visit_days_count,\n" +
            "    CASE\n" +
            "        WHEN b.visit_days_count > 1 THEN\n" +
            "            FLOOR(DATEDIFF(b.last_visit_date, b.first_visit_date) / (b.visit_days_count - 1))\n" +
            "        ELSE 0\n" +
            "    END AS avg_visit_interval,\n" +
            "    md.main_diagnose,\n" +
            "    COALESCE(ps.total_prescription_count, 0) AS total_prescription_count,\n" +
            "    COALESCE(ps.total_medicine_count, 0) AS total_medicine_count\n" +
            "FROM base b\n" +
            "LEFT JOIN age_grouped ag ON b.patient_id = ag.patient_id\n" +
            "LEFT JOIN frequent_dept fd ON b.patient_id = fd.patient_id\n" +
            "LEFT JOIN frequent_doctor fdoc ON b.patient_id = fdoc.patient_id\n" +
            "LEFT JOIN main_diagnose md ON b.patient_id = md.patient_id\n" +
            "LEFT JOIN prescription_sum ps ON b.patient_id = ps.patient_id");

 */
