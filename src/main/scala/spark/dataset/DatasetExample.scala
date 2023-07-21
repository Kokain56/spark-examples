package spark.dataset

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object DatasetExample extends App {
  final case class AIJobIndustry(
                                  jobTitle: String,
                                  company: String,
                                  location: String,
                                  companyReviews: String
                                )

  final case class JobsAndCompany(
                                   name: String,
                                   statesType: String,
                                   location: String,
                                   count: Int,
                                   contentType: String
                                 )

  case class NameWithCount(
                            name: String,
                            count: Int
                          )

  case class NameWithLocation(
                               name: String,
                               statesType: String,
                               location: String,
                               count: Int
                             )


  val spark = SparkSession.builder()
    .appName("Main")
    .master("local")
    .getOrCreate()

  //For DataFrame
  val aiJobsIndustryDF: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("multiLine", "true")
    .csv("src/main/resources/AIJobsIndustry.csv")

  val parsingAiJobsIndustryDF = aiJobsIndustryDF.transform(parsing)

  val jobsDF = parsingAiJobsIndustryDF
    .transform(getJobOrCompanyDF(DfColumn.JobTitle))
    .transform(getMaxAndMinDF)

  val companyDF = parsingAiJobsIndustryDF
    .transform(getJobOrCompanyDF(DfColumn.Company))
    .transform(getMaxAndMinDF)

  val unionJobsAndCompanyDF = jobsDF.union((companyDF)) // finalDF

  unionJobsAndCompanyDF.show

  //For DataSet

  import spark.implicits._

  val aiJobIndustryDS = aiJobsIndustryDF.as[AIJobIndustry]
  val parsingAIJobIndustryDS = aiJobIndustryDS.transform(parsingDS)

  val maxJob = getMax("job")(parsingAIJobIndustryDS)

  val maxJobReviewsDS = parsingAIJobIndustryDS
    .transform(getMaxReviews("job", maxJob.name))

  val maxJobLocationDS = maxJobReviewsDS
    .transform(getMaxLocation)

  val minJobLocationDS = maxJobReviewsDS
    .transform(getMinLocation)

  val jobDS = maxJobLocationDS.union(minJobLocationDS)

  val maxCompany = getMax("company")(parsingAIJobIndustryDS)

  val maxCompanyReviewsDS = parsingAIJobIndustryDS
    .transform(getMaxReviews("company", maxCompany.name))

  val maxCompanyLocationDS = maxCompanyReviewsDS
    .transform(getMaxLocation)

  val minCompanyLocationDS = maxCompanyReviewsDS
    .transform(getMinLocation)

  val companyDS = maxCompanyLocationDS.union(minCompanyLocationDS)

  val jobAndCompanyDS = jobDS.union(companyDS)

  jobAndCompanyDS.show

  spark.stop()

  def parsingDS(ds: Dataset[AIJobIndustry]): Dataset[AIJobIndustry] =
    ds
      .filter(col(DfColumn.JobTitle).isNotNull && col(DfColumn.Company).isNotNull && col(DfColumn.CompanyReviews).isNotNull)
      .map { x =>
        val companyReviews = x.companyReviews.replaceAll("\\D+", "")
        val company = x.company.replaceAll("\\W+", "").trim
        AIJobIndustry(x.jobTitle, company, x.location, companyReviews)
      }.filter(col(DfColumn.Company).notEqual("") && col(DfColumn.CompanyReviews).notEqual(""))

  def getRecord(rec: AIJobIndustry, recType: String) =
    recType match {
      case "job" => rec.jobTitle
      case _ => rec.company
    }

  def getMax(recType: String)(ds: Dataset[AIJobIndustry]): NameWithCount =
    ds.groupByKey(getRecord(_, recType)).mapGroups(
      (name, aiJobIndustry) => {
        val count = aiJobIndustry
          .map(x => if (x.companyReviews.nonEmpty) x.companyReviews.toInt else 0)
          .reduce((a, b) => a + b)
        NameWithCount(name, count)
      }
    ).orderBy(desc(DfColumn.Count)).head()

  def getMaxReviews(recType: String, maxValue: String)(ds: Dataset[AIJobIndustry]): Dataset[NameWithLocation] =
    ds
      .filter(x => getRecord(x, recType).equals(maxValue)).groupByKey(x => (getRecord(x, recType), x.location)).mapGroups(
        (jobAndLocation, aiJobIndustry) => {
          val count = aiJobIndustry
            .map(x => if (x.companyReviews.nonEmpty) x.companyReviews.toInt else 0)
            .reduce((a, b) => a + b)
          NameWithLocation(jobAndLocation._1, "job", jobAndLocation._2, count)
        }
      )

  def getMaxLocation(ds: Dataset[NameWithLocation]): Dataset[JobsAndCompany] =
    ds
      .groupByKey(_.name).mapGroups(
        (_, aiJobIndustry) => {
          val result = aiJobIndustry
            .map(x => (x.name, x.statesType, x.location, x.count))
            .reduce((a, b) => if (a._4 > b._4) a else b)
          JobsAndCompany(result._1, result._2, result._3, result._4, "max")
        }
      )

  def getMinLocation(ds: Dataset[NameWithLocation]): Dataset[JobsAndCompany] =
    ds
      .groupByKey(_.name).mapGroups(
        (_, aiJobIndustry) => {
          val result = aiJobIndustry
            .map(x => (x.name, x.statesType, x.location, x.count))
            .reduce((a, b) => if (a._4 < b._4) a else b)
          JobsAndCompany(result._1, result._2, result._3, result._4, "min")
        }
      )

  def getMaxAndMinDF(df: DataFrame): DataFrame = {
    val maxDF = getMaxDF(df).withColumn(DfColumn.CountType, lit("max"))

    val minDF = getMinDF(df).withColumn(DfColumn.CountType, lit("min"))

    val union = maxDF.union(minDF)
    union.select(
      col(DfColumn.Name),
      col(DfColumn.StatsType),
      col(DfColumn.Location).as(DfColumn.Location),
      col(DfColumn.Reviews).as(DfColumn.Count),
      col(DfColumn.CountType)
    )
  }


  def getMaxDF(df: DataFrame): DataFrame = df
    .groupBy(DfColumn.Name, DfColumn.StatsType, DfColumn.Location)
    .agg(sum(col(DfColumn.Reviews)).as(DfColumn.Reviews))
    .groupBy(DfColumn.Name, DfColumn.StatsType)
    .agg(
      sum(col(DfColumn.Reviews)).as(DfColumn.Count),
      max_by(col(DfColumn.Location), col(DfColumn.Reviews)).as(DfColumn.Location),
      max(col(DfColumn.Reviews)).as(DfColumn.Reviews)
    )
    .orderBy(desc(DfColumn.Count))
    .limit(1)

  def getMinDF(df: DataFrame): DataFrame = df
    .groupBy(DfColumn.Name, DfColumn.StatsType, DfColumn.Location)
    .agg(sum(col(DfColumn.Reviews)).as(DfColumn.Reviews))
    .groupBy(DfColumn.Name, DfColumn.StatsType)
    .agg(
      sum(col(DfColumn.Reviews)).as(DfColumn.Count),
      min_by(col(DfColumn.Location), col(DfColumn.Reviews)).as(DfColumn.Location),
      min(col(DfColumn.Reviews)).as(DfColumn.Reviews)
    )
    .orderBy(desc(DfColumn.Count))
    .limit(1)

  def getJobOrCompanyDF(nameColumn: DfColumn.Value)(df: DataFrame): DataFrame =
    df
      .select(
        col(nameColumn).as(DfColumn.Name),
        col(DfColumn.Location).as(DfColumn.Location),
        col(DfColumn.CompanyReviews).cast("int").as(DfColumn.Reviews),
      ).transform(withColumnDF(nameColumn))

  def withColumnDF(nameColumn: DfColumn.Value)(df: DataFrame): DataFrame = {
    val stringNameColumn = nameColumn match {
      case DfColumn.JobTitle => "job"
      case _ => "company"
    }
    df.withColumn(DfColumn.StatsType, lit(stringNameColumn))
  }

  def parsing(df: DataFrame): DataFrame =
    df
      .filter(col(DfColumn.Company).notEqual("") && col(DfColumn.CompanyReviews).notEqual(""))
      .select(
        col(DfColumn.JobTitle),
        trim(regexp_replace(col(DfColumn.Company), "\\W+", "")).as(DfColumn.Company),
        col(DfColumn.Location),
        regexp_replace(col(DfColumn.CompanyReviews), "\\D+", "").as(DfColumn.CompanyReviews)
      ).na.drop

  object DfColumn
    extends DfColumn {
    implicit def valueToString(aVal: DfColumn.Value): String = aVal.toString
  }

  trait DfColumn extends Enumeration {
    val Name,
    Company,
    Count,
    CountType,
    JobTitle,
    Location,
    Reviews,
    StatsType,
    CompanyReviews = Value
  }

}
