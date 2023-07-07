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
    .transform(getJobDF)
    .transform(processingDF)

  val companyDF = parsingAiJobsIndustryDF
    .transform(getCompanyDF)
    .transform(processingDF)

  val unionJobsAndCompanyDF = jobsDF.union((companyDF)) // finalDF

  unionJobsAndCompanyDF.show

  //For DataSet

  import spark.implicits._

  val aiJobIndustryDS = aiJobsIndustryDF.as[AIJobIndustry]
  val parsingAIJobIndustryDS = aiJobIndustryDS.transform(parsingDS)

  val maxJob = getMaxJob(parsingAIJobIndustryDS)

  val maxJobReviewsDS = parsingAIJobIndustryDS
    .transform(getMaxJobReviews)

  val maxJobLocationDS = maxJobReviewsDS
    .transform(getMaxLocation)

  val minJobLocationDS = maxJobReviewsDS
    .transform(getMinLocation)

  val jobDS = maxJobLocationDS.union(minJobLocationDS)

  val maxCompany = getMaxCompany(parsingAIJobIndustryDS)

  val maxCompanyReviewsDS = parsingAIJobIndustryDS
    .transform(getMaxCompanyReviews)

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

  def getMaxCompany(ds: Dataset[AIJobIndustry]): NameWithCount =
    ds.groupByKey(_.company).mapGroups(
      (name, aiJobIndustry) => {
        val count = aiJobIndustry
          .map(x => if (x.companyReviews.nonEmpty) x.companyReviews.toInt else 0)
          .reduce((a, b) => a + b)
        NameWithCount(name, count)
      }
    ).orderBy(desc(DfColumn.Count)).head()

  def getMaxJob(ds: Dataset[AIJobIndustry]): NameWithCount =
    ds.groupByKey(_.jobTitle).mapGroups(
      (name, aiJobIndustry) => {
        val count = aiJobIndustry
          .map(x => if (x.companyReviews.nonEmpty) x.companyReviews.toInt else 0)
          .reduce((a, b) => a + b)
        NameWithCount(name, count)
      }
    ).orderBy(desc(DfColumn.Count)).head()

  def getMaxJobReviews(ds: Dataset[AIJobIndustry]): Dataset[NameWithLocation] =
    ds
      .filter(x => x.jobTitle.equals(maxJob.name)).groupByKey(x => (x.jobTitle, x.location)).mapGroups(
        (jobAndLocation, aiJobIndustry) => {
          val count = aiJobIndustry
            .map(x => if (x.companyReviews.nonEmpty) x.companyReviews.toInt else 0)
            .reduce((a, b) => a + b)
          NameWithLocation(jobAndLocation._1, "job", jobAndLocation._2, count)
        }
      )

  def getMaxCompanyReviews(ds: Dataset[AIJobIndustry]): Dataset[NameWithLocation] =
    ds
      .filter(x => x.company.equals(maxCompany.name)).groupByKey(x => (x.company, x.location)).mapGroups(
        (companyAndLocation, aiJobIndustry) => {
          val count = aiJobIndustry
            .map(x => if (x.companyReviews.nonEmpty) x.companyReviews.toInt else 0)
            .reduce((a, b) => a + b)
          NameWithLocation(companyAndLocation._1, "company", companyAndLocation._2, count)
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

  def processingDF(df: DataFrame): DataFrame = {

    val minAndMaxLocationDS = df
      .groupBy(DfColumn.name, DfColumn.StatsType, DfColumn.Location)
      .agg(sum(col(DfColumn.Reviews)).as(DfColumn.Reviews))
      .groupBy(DfColumn.name, DfColumn.StatsType)
      .agg(
        sum(col(DfColumn.Reviews)).as(DfColumn.Count),
        min_by(col(DfColumn.Location), col(DfColumn.Reviews)).as(DfColumn.LocationMin),
        max_by(col(DfColumn.Location), col(DfColumn.Reviews)).as(DfColumn.LocationMax),
        min(col(DfColumn.Reviews)).as(DfColumn.ReviewsMin),
        max(col(DfColumn.Reviews)).as(DfColumn.ReviewsMax)
      )
      .orderBy(desc(DfColumn.Count))
      .limit(1)

    minAndMaxLocationDS.select(
      col(DfColumn.name),
      col(DfColumn.StatsType),
      col(DfColumn.LocationMin).as(DfColumn.Location),
      col(DfColumn.ReviewsMin).as(DfColumn.Count),
      lit("min").as(DfColumn.CountType)
    ).union(
      minAndMaxLocationDS.select(
        col(DfColumn.name),
        col(DfColumn.StatsType),
        col(DfColumn.LocationMax).as(DfColumn.Location),
        col(DfColumn.ReviewsMax).as(DfColumn.Count),
        lit("max").as(DfColumn.CountType)
      )
    )
  }

  def getJobDF(df: DataFrame): DataFrame =
    df
      .select(
        col(DfColumn.JobTitle).as(DfColumn.name),
        col(DfColumn.Location).as(DfColumn.Location),
        col(DfColumn.CompanyReviews).cast("int").as(DfColumn.Reviews),
      ).withColumn(DfColumn.StatsType, lit("job"))

  def getCompanyDF(df: DataFrame): DataFrame =
    df
      .select(
        col(DfColumn.Company).as(DfColumn.name),
        col(DfColumn.Location).as(DfColumn.Location),
        col(DfColumn.CompanyReviews).cast("int").as(DfColumn.Reviews),
      ).withColumn(DfColumn.StatsType, lit("company"))
      .where(col(DfColumn.name).notEqual(""))

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
    val name,
    Company,
    Count,
    CountType,
    JobTitle,
    Location,
    LocationMax,
    LocationMin,
    Reviews,
    ReviewsMax,
    ReviewsMin,
    StatsType,
    CompanyReviews = Value
  }

}
