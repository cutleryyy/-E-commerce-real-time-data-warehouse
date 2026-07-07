package test;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class hive_dwd {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("hive_dwd")
                .master("local[*]")
//                .master("spark://192.168.94.130:7077")
                .enableHiveSupport().getOrCreate();
//      开启AQE：自动调整shuffle分区、自动优化join、缓解数据倾斜
        spark.conf().set(
                "spark.sql.adaptive.enabled",
                "true"
        );
//        spark.conf().set("spark.sql.dialect", "hive");
////      调整shuffle后的分区数量，避免出现大量空闲task，浪费资源==df.repartition(10)
        spark.conf().set(
                "spark.sql.shuffle.partitions",
                "10"
        );
        spark.conf().set("spark.sql.adaptive.coalescePartitions.enabled", "true");
        // 启用 Hive 动态分区支持
        spark.conf().set("hive.exec.dynamic.partition", "true");
        spark.conf().set("hive.exec.dynamic.partition.mode", "nonstrict");
        // 优化动态分区写入性能
        spark.conf().set("spark.sql.sources.partitionOverwriteMode", "dynamic");
        spark.conf().set("spark.sql.adaptive.skewJoin.enabled", "true");
//      ======================创建患者dwd表===============================
/*
        Dataset<Row> df_patient = spark.table("medical_spark.ods_patient")
                .filter("id IS NOT NULL");
        df_patient.createOrReplaceTempView("patient");

        spark.sql(" CREATE EXTERNAL TABLE if not exists medical_spark.dwd_patient (\n" +
                "    patient_id INT comment '患者id',\n" +
                "    patient_name STRING comment '患者姓名',\n" +
                "    id_card STRING comment '身份证号', \n" +
                "    phone STRING comment '手机号', \n" +
                "    gender STRING comment '性别',\n" +
                "     age INT comment '年龄',\n" +
                "    address STRING comment '地址',\n" +
                "    create_time TIMESTAMP COMMENT '创建时间'\n" +
                ")\n" +
                "stored as orc\n" +
                "LOCATION '/user/hive/warehouse/medical_spark.db/dwd_patient'\n" +
                "TBLPROPERTIES (\n" +
                "    'orc.compress' = 'SNAPPY',\n" +
                "    'comment' = '患者基本信息明细数据'\n" +
                ");"
        );

        spark.sql("INSERT OVERWRITE TABLE medical_spark.dwd_patient\n" +
                "SELECT id as patient_id,\n" +
                "       case when length(patient_name)=2\n" +
                "       then concat(substring(patient_name,1,1),'*')\n" +
                "           when length(patient_name)>2\n" +
                "       then CONCAT(\n" +
                "        SUBSTRING(patient_name, 1, 1), \n" +
                "        REPEAT('*', length(patient_name) - 2),\n" +
                "        SUBSTRING(patient_name, length(patient_name), 1)\n" +
                "       )\n" +
                "           end\n" +
                "           as patient_name,\n" +
                "       concat(substring(id_card,1,6),'********',substring(id_card,15,4))\n" +
                "       as id_card,\n" +
                "       CONCAT(SUBSTR(phone,1,3),'****',SUBSTR(phone,8,4)) AS phone_number,\n" +
                "       case when gender in ('1','男','male')then '男'\n" +
                "            when gender in ('2','女','female')then '女'\n" +
                "           else '其他'\n" +
                "       end as gender,\n" +
                " FLOOR(months_between(current_date(),birth)/12) AS age,\n" +
                "    address,\n" +
                "    create_time\n" +
                "FROM patient;");

        spark.sql("SELECT COUNT(*) FROM medical_spark.dwd_patient").show();
 */

//        =======================科室表 =============================
/*
                spark.sql("drop table if exists medical_spark.department");
        spark.sql("create external table if not exists medical_spark.department(\n" +
                "    dep_id BIGINT comment '科室id',\n" +
                "    dept_name STRING comment '科室名称',\n" +
                "    dept_type STRING COMMENT '科室类型',\n" +
                "    create_time TIMESTAMP COMMENT '创建时间'\n" +
                ")" +
                "stored as orc\n" +
                "LOCATION '/user/hive/warehouse/medical_spark.db/department'\n" +
                "TBLPROPERTIES (\n" +
                "    'orc.compress' = 'SNAPPY',\n" +
                "    'comment' = '科室基本信息明细数据'\n" +
                ")");
        spark.sql(" INSERT OVERWRITE TABLE medical_spark.department " +
                "select dep_id,\n" +
                "       dept_name,\n" +
                "       dept_type,\n" +
                "       create_time from medical_dwd.dwd_department\n" +
                "where dep_id is not null; ");

//        ========================医生表==========================
        spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS medical_spark.dwd_doctor (\n" +
                "   doctor_id STRING COMMENT '医生id',\n" +
                "   doctor_name STRING COMMENT '医生姓名',\n" +
                "   dept_id BIGINT COMMENT '所属科室id',\n" +
                "   dept_name STRING COMMENT '所属科室名称',\n" +
                "   title STRING COMMENT '职务',\n" +
                "   id_card STRING COMMENT '身份证号',\n" +
                "   status STRING COMMENT '在职状态',\n" +
                "   create_time TIMESTAMP COMMENT '创建时间'\n" +
                ") \n" +
                "STORED AS ORC\n" +
                "LOCATION '/user/hive/warehouse/medical_spark.db/dwd_doctor'\n" +
                "TBLPROPERTIES (\n" +
                "    'orc.compress' = 'SNAPPY',\n" +
                "    'comment' = '医生基本信息明细数据'\n" +
                ")");

        spark.sql("INSERT OVERWRITE TABLE medical_spark.dwd_doctor\n" +
                "SELECT \n" +
                "    doctor_id,\n" +
                "    doctor_name,\n" +
                "    dept_id,\n" +
                "    dept_name,\n" +
                "    title,\n" +
                "    id_card,\n" +
                "    status,\n" +
                "    create_time\n" +
                "FROM medical_dwd.dwd_doctor\n" +
                "WHERE doctor_id IS NOT NULL");


        spark.sql("SELECT *FROM medical_spark.dwd_doctor limit 10").show(false);
 */
//        ======================药品表===========================
        /*
//        spark.sql("CREATE external TABLE medical_spark.dwd_medicine(\n" +
//                "    med_code bigint comment '药品编码',\n" +
//                "    med_name STRING comment '药品名称',\n" +
//                "    specification STRING comment '药品规格',\n" +
//                "    unit STRING comment '单位',\n" +
//                "    manufacturer STRING comment '生产厂商',\n" +
//                "    price DECIMAL(10,2) comment '单价',\n" +
//                "    stock INT comment '库存数量',\n" +
//                "    stock_status string comment '库存情况',\n" +
//                "    create_time TIMESTAMP COMMENT '创建时间'\n" +
//                ")\n" +
//                "stored as orc\n" +
//                "LOCATION '/user/hive/warehouse/medical_spark.db/dwd_medicine'\n" +
//                "TBLPROPERTIES (\n" +
//                "    'orc.compress' = 'SNAPPY',\n" +
//                "    'comment' = '药品基本信息明细数据'\n" +
//                ")");
//        spark.sql("insert overwrite table medical_spark.dwd_medicine\n" +
//                "select med_code,\n" +
//                "       med_name,\n" +
//                "       specification,\n" +
//                "       unit,\n" +
//                "       manufacturer,\n" +
//                "       price,\n" +
//                "       stock,\n" +
//                "       case when stock < 10 then '即将缺货'\n" +
//                "           when stock<100 then '库存紧张'\n" +
//                "           else '库存充足' end\n" +
//                "           as stock_status ,\n" +
//                "        create_time\n" +
//                "from medical_spark.ods_medicine\n" +
//                "where\n" +
//                "    med_code is not null");
//
//        spark.sql("SELECT *FROM medical_spark.dwd_medicine limit 10").show(false);
         */

//        ====================== 门诊表==========================
        /*
        spark.sql("drop table medical_spark.dwd_clinic");
        spark.sql("CREATE external TABLE if not exists medical_spark.dwd_clinic_test (\n" +
                "    clinic_id INT comment '就诊ID',\n" +
                "    patient_id INT comment '患者ID',\n" +
                "    patient_name string comment '患者姓名',\n" +
                "    patient_age int comment '患者年龄',\n" +
                "    patient_gender string comment '患者性别',\n" +
                "    dept_id BIGINT COMMENT '科室ID',\n" +
                "    dept_name string comment '科室名',\n" +
                "    doctor_id STRING comment '医生id',\n" +
                "    doctor_name string comment '医生姓名',\n" +
                "    visit_date DATE comment '就诊日期',\n" +
                "    visit_time STRING comment '就诊时间',\n" +
                "    visit_hour int comment '就诊小时',\n" +
                "    visit_weekday string comment '就诊星期',\n" +
                "    diagnosis STRING comment '诊断结果',\n" +
                "    fee DECIMAL(10,2) comment '就诊费用',\n" +
                "    payment_type STRING comment '支付方式',\n" +
                "    create_time TIMESTAMP COMMENT '创建时间'\n" +
                ")PARTITIONED BY (dt STRING)\n" +
                "stored as orc\n" +
                "LOCATION '/user/hive/warehouse/medical_spark.db/dwd_clinic_test'\n" +
                "TBLPROPERTIES (\n" +
                "    'orc.compress' = 'SNAPPY',\n" +
                "    'comment' = '门诊基本信息明细数据'\n" +
                ")");

        spark.sql("insert into table dwd_clinic\n" +
                "select\n" +
                "    id as clinic_id,\n" +
                "    c.patient_id,\n" +
                "    p.age,\n" +
                "    p.gender,\n" +
                "    c.dept_id,\n" +
                "    d.dept_name,\n" +
                "    d.doctor_id,\n" +
                "    d.doctor_name,\n" +
                "    visit_date,\n" +
                "    visit_time,\n" +
                "    cast(substring(visit_time,1,2) as int) as visit_hour,\n" +
                "    CASE WHEN dayofweek(visit_date) = 1 THEN 7 ELSE dayofweek(visit_date)-1 END AS visit_weekday,\n" +
                "    diagnosis,\n" +
                "    fee,\n" +
                "    payment_type,\n" +
                "    c.create_time,\n" +
                "    c.dt"+
                "from medical_spark.ods_clinic c\n" +
                "left join medical_spark.dwd_doctor d on c.dept_id = d.dept_id\n" +
                " left join medical_spark.dwd_patient p on c.patient_id = p.patient_id\n" +
                "where\n" +
                "    id is not null\n" +
                "    and visit_date is not null\n");
        long start = System.currentTimeMillis();
        Dataset<Row> clinic = spark.table("medical_spark.ods_clinic");
        Dataset<Row> doctor = spark.table("medical_spark.dwd_doctor");
        Dataset<Row> patient = spark.table("medical_spark.dwd_patient");
        Dataset<Row> result = clinic.join(broadcast(doctor),clinic.col("dept_id").equalTo(doctor.col("dept_id")),"left")
                .join(broadcast(patient),clinic.col("patient_id").equalTo(patient.col("patient_id")),"left")
                .where(clinic.col("id").isNotNull().and(clinic.col("visit_date").isNotNull()))
                .select(
                        clinic.col("id").alias("clinic_id"),
                        clinic.col("patient_id"),
                        patient.col("patient_name"),
                        patient.col("age").alias("patient_age"),
                        patient.col("gender").alias("patient_gender"),
                        clinic.col("dept_id"),
                        doctor.col("dept_name"),
                        doctor.col("doctor_id"),
                        doctor.col("doctor_name"),
                        clinic.col("visit_date").cast("date"),
                        clinic.col("visit_time"),
                        substring(clinic.col("visit_time"),1,2).cast(("int")).alias("visit_hour"),
                        when(dayofweek(clinic.col("visit_date")).equalTo(1),7)
                                .otherwise(dayofweek(clinic.col("visit_date")).minus(1)).alias("visit_weekday"),
                        clinic.col("diagnosis"),
                        clinic.col("fee"),
                        clinic.col("payment_type"),
                        clinic.col("create_time").cast("timestamp"),
                        clinic.col("dt")
                );

                result.write().mode(SaveMode.Overwrite).insertInto("medical_spark.dwd_clinic");


        long end = System.currentTimeMillis();
        System.out.println("任务执行耗时：" + (end - start) + " ms");
        System.out.println("约 " + (end - start) / 1000.0 + " 秒");
        System.out.println("=================================");
        spark.sql("SELECT *FROM medical_spark.dwd_clinic_test limit 10").show(false);
        */
//        ====================== 处方表==========================

        final long start = System.currentTimeMillis();
        /*
        spark.sql("drop table IF EXISTS medical_spark.dwd_prescription");
        spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS medical_spark.dwd_prescription(\n" +
                "    id BIGINT COMMENT '处方ID',\n" +
                "    visit_id BIGINT COMMENT '就诊ID',\n" +
                "    patient_id BIGINT COMMENT '患者ID',\n" +
                "    patient_name STRING COMMENT '患者姓名',\n" +
                "    doctor_id STRING COMMENT '医生ID',\n" +
                "    doctor_name STRING COMMENT '医生姓名',\n" +
                "    dept_id BIGINT COMMENT '科室ID',\n" +
                "    dept_name STRING COMMENT '科室名称',\n" +
                "    medicine_code STRING COMMENT '药品编码',\n" +
                "    medicine_name STRING COMMENT '药品名称',\n" +
                "    dosage STRING COMMENT '剂量',\n" +
                "    frequency STRING COMMENT '频次',\n" +
                "    duration STRING COMMENT '疗程',\n" +
                "    unit_price DECIMAL(10,2) COMMENT '单价',\n" +
                "    quantity INT COMMENT '数量',\n" +
                "    total_price DECIMAL(10,2) COMMENT '总金额'\n" +
                ")partitioned by (visit_date string)\n" +
                "stored as orc\n" +
                "location '/user/hive/warehouse/medical_spark.db/dwd_prescription'\n" +
                "TBLPROPERTIES (\n" +
                "    'orc.compress' = 'SNAPPY',\n" +
                "    'comment' = '处方基本信息明细数据'\n" +
                ")");

        Dataset<Row> clinic = spark.table("medical_ods.ods_clinic");
        Dataset<Row> doctor = spark.table("medical_ods.ods_doctor");
        Dataset<Row> patient = spark.table("medical_ods.ods_patient");
        Dataset<Row> prescription = spark.table("medical_ods.ods_prescription");
        Dataset<Row> department = spark.table("medical_ods.department");
        Dataset<Row> medicine = spark.table("medical_ods.ods_medicine");

        Dataset<Row> result_clinic = clinic.join(broadcast(patient),clinic.col("patient_id").equalTo(patient.col("id")),"inner")
                .join(broadcast(doctor),clinic.col("doctor_id").equalTo(doctor.col("id")),"left")
                .join(broadcast(department),clinic.col("dept_id").equalTo(department.col("id")),"left")
                .where(doctor.col("id").isNotNull().and(department.col("id").isNotNull()))
                .select(clinic.col("id").alias("clinic_id"),
                        clinic.col("patient_id"),
                        patient.col("patient_name"),
                        clinic.col("doctor_id"),
                        doctor.col("doctor_name"),
                        clinic.col("dept_id"),
                        department.col("dept_name")
                        );.explain(true)

        Dataset<Row> result = prescription.join(broadcast(result_clinic),prescription.col("visit_id").equalTo(result_clinic.col("clinic_id")),"left")
                .join(broadcast(medicine),prescription.col("medicine_id").equalTo(medicine.col("med_code")),"left")
                        .select(prescription.col("id").alias("prescription_id"),
                                prescription.col("visit_id"),
                                result_clinic.col("patient_id"),
                                result_clinic.col("patient_name"),
                                result_clinic.col("doctor_id"),
                                result_clinic.col("doctor_name"),
                                result_clinic.col("dept_id"),
                                result_clinic.col("dept_name"),
                                when(medicine.col("med_code").isNull(),"未知").otherwise(medicine.col("med_code")).alias("medicine_code"),
                                when(medicine.col("med_name").isNull(),"未知药品").otherwise(medicine.col("med_name")).alias("medicine_name"),
                                prescription.col("dosage"),
                                prescription.col("frequency"),
                                prescription.col("duration"),
                                medicine.col("price").alias("unit_price"),
                                prescription.col("quantity"),
                                prescription.col("total_price")
                        );

        result.write().mode(SaveMode.Overwrite).insertInto("medical_dwd.dwd_prescription_test_spark");

        spark.sql("select * from medical_dwd.dwd_prescription_test_spark limit 10").show();
        spark.sql("select count(*) as null_count from medical_dwd.dwd_prescription_test_spark where doctor_id is null or medicine_code='未知'").show();
         */


        final long end = System.currentTimeMillis();
        long time = end - start;

        System.out.println("执行时间为：" + time + "ms");

    }
}
