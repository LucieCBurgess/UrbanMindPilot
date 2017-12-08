package dev.data_load

import scala.collection.immutable.Map

/**
  * Created by lucieburgess on 06/12/2017.
  * Unresolved:
  * Q3, "In a village" - no response for this in the pdf, but is a response in the raw data file
  * Numbering in the map below relates to questions that do not already have numerical values
  */
object ScoreMap {

  val scoreMap = Map("Male" -> 201, "Female" -> 202, "Other" -> 203,
    "In the country" -> 301, "In a town" -> 302, "In a city" -> 303, "Multiple places" -> 304,
    "Secondary school" -> 501, "Training college" -> 502, "Apprenticeship" -> 503, "University" -> 504, "Doctoral degree" -> 505,
    "Student" -> 601, "Employed" -> 602, "Self-employed" -> 603, "Retired" -> 604, "Unemployed" -> 605,
    "Never" -> 270, "Monthly or less" -> 271, "2-4 times per month" -> 272, "2-3 times per week" -> 273, "4+ times per week" -> 274,
    "1-2" -> 280, "3-4" -> 281, "5-6" -> 282, "7-9" -> 283, "10+" -> 284,
    "Never" -> 290, "Less than monthly" -> 291, "Weekly" -> 293, "Daily or almost daily" -> 294)

}

//"Rarely/Never" -> 1, "Occasionally" -> 2, "Often" -> 3, "Almost Always/Always" -> 4,
//"None of the time" -> 1, "Rarely" -> 2, "Some of the time" -> 3, "Often" -> 4, "All of the time" -> 5,
// Often maps to different keys