package dev.data_load

import org.apache.spark.sql.types._

/**
  * Created by lucieburgess on 20/10/2017.
  * An object to set the schema of the dataset based on the Urban Mind pilot data
  */

object Schema_definition {

  val pilotDataSchema: StructType = StructType(Array(
      StructField("time", IntegerType, nullable=true),
      StructField("id", StringType, nullable=true),
      StructField("age", IntegerType, true),
      StructField("gender", StringType, true),
      StructField("occupation", StringType, true),
      StructField("education", StringType, true),
      StructField("impulse", StringType, true),
      StructField("grow_up", StringType, true),
      StructField("indoors_outdoors", StringType, true),
      StructField("trees", StringType, true),
      StructField("birds", StringType, true),
      StructField("water", StringType, true),
      StructField("sky", StringType, true),
      StructField("contact", StringType, true),
      StructField("OUTTREES", StringType, true),
      StructField("mom_optimistic", StringType, true),
      StructField("mom_useful", StringType, true),
      StructField("mom_relaxed", StringType, true),
      StructField("mom_int_other_ppl", StringType, true),
      StructField("mom_energy", StringType, true),
      StructField("mom_probs", StringType, true),
      StructField("mom_think", StringType, true),
      StructField("mom_energy", StringType, true),
      StructField("mom_feel_good", StringType, true),
      StructField("mom_close_ppl", StringType, true),
      StructField("mom_confident", StringType, true),
      StructField("mom_make_mind", StringType, true),
      StructField("mom_loved", StringType, true),
      StructField("mom_int_things", StringType, true),
      StructField("mom_cheerful", StringType, true),
      StructField("bas_optimistic", StringType, true),
      StructField("bas_useful", StringType, true),
      StructField("bas_relaxed", StringType, true),
      StructField("bas_int_other_ppl", StringType, true),
      StructField("bas_energy", StringType, true),
      StructField("bas_probs", StringType, true),
      StructField("bas_think", StringType, true),
      StructField("bas_energy", StringType, true),
      StructField("bas_feel_good", StringType, true),
      StructField("bas_close_ppl", StringType, true),
      StructField("bas_confident", StringType, true),
      StructField("bas_make_mind", StringType, true),
      StructField("bas_loved", StringType, true),
      StructField("bas_int_things", StringType, true),
      StructField("bas_cheerful", StringType, true),
      StructField("focused", StringType, true),
      StructField("plan_work", StringType, true),
      StructField("plan_events", StringType, true),
      StructField("think_care", StringType, true),
      StructField("self_control", StringType, true),
      StructField("jump_int", StringType, true),
      StructField("stop_think", StringType, true),
      StructField("get_out", StringType, true),
      StructField("self_mental", StringType, true),
      StructField("self_phys", StringType, true),
      StructField("mom_health", IntegerType, true),
      StructField("bas_health", IntegerType, true),
        StructField("impulsetotal", IntegerType, true),
        StructField("ntrees", StringType, true),
        StructField("nbirds", StringType, true),
        StructField("nsky", StringType, true),
        StructField("ncontact", StringType, true),
        StructField("lat", DoubleType, true),
        StructField("lon", DoubleType, true)))
  }




