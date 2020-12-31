package com.upgrad.casestudy.course6;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;

//import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
//import org.apache.spark.mllib.recommendation.Rating;
//import org.apache.spark.mllib.recommendation.ALS;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.catalyst.expressions.Concat;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.functions.broadcast;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import scala.Function1;
import scala.Tuple2;

import static org.apache.spark.sql.functions.col;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

public class ClusteringSaavnCaseStudyDriver {

	static String accessKey = "AKIAUD5HVR7H74QGWA6F";
	static String secretKey = "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";

	private static String sourceFolderOrBucket = "bigdataanalyticsupgrad";
	private static String clckStreamEndPoint = "s3a://bigdataanalyticsupgrad/activity/sample100mb.csv";
	private static String songMetaDataEndPoint = "newmetadata" ;//"s3a://bigdataanalyticsupgrad/newmetadata/*";
	private static String notificationClkEndPoint =  "notification_clicks"; //"s3a://bigdataanalyticsupgrad/notification_clicks/*";
	private static String notificationArtistEndPoint =  "notification_actor";//"s3a://bigdataanalyticsupgrad/notification_actor/*";
	private static String mode = null;
	private static boolean isConnetingS3 = true;

	public static void main(String[] args) {
		ClusteringSaavnCaseStudyDriver songRecomendation = new ClusteringSaavnCaseStudyDriver();
		//SparkSession sparksession = new SparkSession.Builder().master("local[*]").appName("clustering").getOrCreate();
		SparkSession sparksession = null;// SparkSession.builder().appName("clustering").getOrCreate(); // for ec2 running
		//sparksession.sparkContext().setLogLevel("ERROR");
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		
		if(args != null && args[0] != null && args[0].equalsIgnoreCase("local"))
		{
		  mode = args[0].toLowerCase().trim(); isConnetingS3 = false;
		}
		else { isConnetingS3 = true;}
		
		if(isConnetingS3) 
			sparksession = SparkSession.builder().appName("clustering").getOrCreate(); // for ec2 running
		else 
			sparksession = new SparkSession.Builder().master("local[*]").appName("clustering").getOrCreate(); //local
		
		// taking arguments from command line agruments when running local 
		if ("local".equalsIgnoreCase(mode)) {
			// Supplies the end points from the data need to be read
			// This will be source directory if local and bigdataanlytics if it is S3
			sourceFolderOrBucket = args[1];
			// This will be click stream file if local and activity/sample100mb.csv if it is
			// S3
			clckStreamEndPoint = args[2];
			// This will be meta data folder if local and newmetadata if it is s3
			songMetaDataEndPoint = args[3];
			// This will be notification Clicks folder if local and notification_click if it
			// is s3
			notificationClkEndPoint = args[4];
			// This will be notification Actor folder if local and notification_actor if it
			// is s3
			notificationArtistEndPoint = args[5];
		}

		if (isConnetingS3 && !"local".equalsIgnoreCase(mode)) {
			sparksession.sparkContext().hadoopConfiguration().set("spark.sql.broadcastTimeout", "36000");
			sparksession.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", accessKey);
			sparksession.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", secretKey);
			sparksession.sparkContext().hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com");
			
		}
		songRecomendation.start(sparksession);
	}

	private void start(SparkSession sparkSession) {
		if(isConnetingS3)
			System.out.println("***********************RUNNING IN EC2 *****************");
		else
			System.out.println("***********************RUNNING LOCALLY*****************");
		System.out.println("**********************program starts *****************************" + printCurrentTime());

		System.out.println("**********************read clickstream starts *****************************");
		Dataset<Row> clusteredUsersActivityDF = readUsersStreamActivity(sparkSession);
		
		System.out.println("Step 2 - Read Meta Data From file***********************");
		Dataset<Row> metaDataDf = readMetaDataOfSongs(sparkSession);
		
		
		System.out.println("Step 3 - Map Artists to Cluster*******************************");
       	Dataset<Row> clusteredUsersArtistMappingDF = mapPopularArtistToUserCluster(clusteredUsersActivityDF,metaDataDf);
       	
       	System.out.println("Step 4 - Map Artists, Cluster and Userids**************************");
       clusteredUsersArtistMappingDF = mapArtistClusterUsers(clusteredUsersActivityDF,clusteredUsersArtistMappingDF);
      //if(isConnetingS3)  writeDataFrame(clusteredUsersArtistMappingDF,0,sparkSession,"UserClusterArtist");
 
       System.out.println("Step 5 - Get Notifications and Mapping with User,Artist,Cluster ************");
      Dataset<Row> notificationsToArtistClusterUsersMappingDF = mapNotificationsToUserArtistCluster(sparkSession,clusteredUsersArtistMappingDF);
	  
       System.out.println("Step 6 - Get Notification Clicks By Cluster****************");
		Dataset<Row> notificiationsClicksToClusterMappingDF = mapNotificationClicksToUser(sparkSession,notificationsToArtistClusterUsersMappingDF); 
		if(isConnetingS3)  writeDataFrame(notificiationsClicksToClusterMappingDF, 1, sparkSession, ""); 
		
		System.out.println("Step 7 - Find CTR to get top 5 notifications***********");
		Dataset<Row> top5Notifications = doAnalysis(notificationsToArtistClusterUsersMappingDF,notificiationsClicksToClusterMappingDF);
		if(isConnetingS3)  writeDataFrame(top5Notifications, 2, sparkSession, "CTR");
	
		sparkSession.stop();
		sparkSession.close();
		
		System.out.println("**********************program ends *****************************  " + printCurrentTime());
	}

	private Dataset<Row> mapNotificationClicksToUser(SparkSession sparkSession,
			Dataset<Row> notificationsToArtistClusterUsersMappingDF) {
		System.out.println("mapNotificationClicksToUser :STARTS -->" + printCurrentTime());
		System.out.println("Reading Notification User Clicks");
		Dataset<Row> notificationUserClicksDF = sparkSession.read().option("header", false).option("inferSchema", true)
				.csv(readFromFiles(sourceFolderOrBucket, notificationClkEndPoint))
				.toDF(new String[] { "NotificationId", "UserId", "Date" }).drop("Date")
				.select(col("NotificationId").cast(DataTypes.LongType), col("UserId")).na().drop();

		// Pick up the notification clicks and mapping against clustered users to
		// generate the clicks against every user and cluster mapped with notification
		System.out.println("*******Mapping the notifcation clicks to notification,artist,user,clusters******* took 30 mins below join");
		notificationUserClicksDF = notificationsToArtistClusterUsersMappingDF
				.join(notificationUserClicksDF,
						(notificationsToArtistClusterUsersMappingDF.col("NotificationId")
								.equalTo(notificationUserClicksDF.col("NotificationId")))
										.and(notificationsToArtistClusterUsersMappingDF.col("UserId")
												.equalTo(notificationUserClicksDF.col("UserId"))))
				.select(notificationUserClicksDF.col("NotificationId"), notificationsToArtistClusterUsersMappingDF.col("ArtistId"),
						notificationUserClicksDF.col("UserId"), notificationsToArtistClusterUsersMappingDF.col("cluster"),
						notificationsToArtistClusterUsersMappingDF.col("artistCluster"));
		//notificationUserClicksDF.distinct("");
		//notificationUserClicksDF.show();
		System.out.println("mapNotificationClicksToUser :ENDS -->" + printCurrentTime());
		return notificationUserClicksDF;
	}

	private Dataset<Row> mapNotificationsToUserArtistCluster(SparkSession sparkSession,Dataset<Row> clusteredUsersArtistMappingDF) {
		System.out.println("mapNotificationsToUserArtistCluster :STARTS -->" + printCurrentTime());
		Dataset<Row> notificationByArtistDF = sparkSession.read().option("header", false).option("inferSchema", true)
				.csv(readFromFiles(sourceFolderOrBucket, notificationArtistEndPoint))
				.toDF(new String[] { "NotificationId", "ArtistId" })
				.select(col("NotificationId").cast(DataTypes.LongType), col("ArtistId").cast(DataTypes.LongType)).na()
				.drop();

		// Map notifications and artist cluster with ArtistID
		System.out.println("Notifications to Popular artist and Cluster Mapping ***********");
		notificationByArtistDF = notificationByArtistDF.join(clusteredUsersArtistMappingDF, "ArtistId")
				.select("NotificationId", "ArtistId", "UserId", "cluster", "artistCluster");
		//notificationByArtistDF.show();
		System.out.println("mapNotificationsToUserArtistCluster :ENDS -->" + printCurrentTime());
		return notificationByArtistDF;
	}

		private Dataset<Row> mapArtistClusterUsers(Dataset<Row> clusteredUsersActivityDf,
			Dataset<Row> clusteredUsersArtistMappingDf) {
			System.out.println("mapArtistClusterUsers :STARTS -->" + printCurrentTime());
			printCurrentTime();
			Dataset<Row> resultdf = clusteredUsersActivityDf.join(clusteredUsersArtistMappingDf,"cluster")
					.select(clusteredUsersActivityDf.col("UserId"),clusteredUsersActivityDf.col("cluster"),clusteredUsersArtistMappingDf.col("ArtistId"),clusteredUsersArtistMappingDf.col("artistCluster"));

			System.out.println("mapArtistClusterUsers :ENDS -->" + printCurrentTime());
			return resultdf;
	}

		private Dataset<Row> mapPopularArtistToUserCluster(Dataset<Row> clusteredUsersActivityDf, Dataset<Row> metaDataDf) {
			System.out.println("mapPopularArtistToUserCluster :STARTS -->" + printCurrentTime());
			//Find the hits for every cluster & Artist Combination
			Dataset<Row> clusterArtistMappingDf = clusteredUsersActivityDf.join(metaDataDf,"SongId").drop(clusteredUsersActivityDf.col("SongId"));
			//clusterArtistMappingDf.show();

		/*	clusterArtistMappingDf = clusterArtistMappingDf.select("ArtistId","cluster")
									.groupBy("ArtistId","cluster")
									.count().withColumnRenamed("count", "artistHits").na().drop();*/ //- removed this as no null was found here
			clusterArtistMappingDf = clusterArtistMappingDf.select("ArtistId","cluster")
					.groupBy("cluster","ArtistId")
					.count().withColumnRenamed("count", "artistHits").na().drop();
			System.out.println("**************After joining clusterreduserActivity & metaData ***********************" + printCurrentTime());
			//clusterArtistMappingDf.show(); // taking too much time
			
			System.out.println("Mapping cluster for maxHits Artist ");
			//Find the cluster artist Row with maximum Hits only for every artist
			WindowSpec clusterWindowByArtist = org.apache.spark.sql.expressions.Window.partitionBy(clusterArtistMappingDf.col("cluster"))
												.orderBy(clusterArtistMappingDf.col("artistHits").desc());
			Dataset<Row> clusterMaxArtistMappingDf = clusterArtistMappingDf.withColumn("maxArtistRowNumber", 
													org.apache.spark.sql.functions.row_number().over(clusterWindowByArtist))
													.where("maxArtistRowNumber = 1");
			
			
			
			clusterMaxArtistMappingDf = clusterMaxArtistMappingDf.drop("maxArtistRowNumber","artistHits");
			
			WindowSpec artistClustering = org.apache.spark.sql.expressions.Window.orderBy(clusterArtistMappingDf.col("ArtistId"));
			clusterMaxArtistMappingDf = clusterMaxArtistMappingDf.withColumn("artistCluster", org.apache.spark.sql.functions.dense_rank().over(artistClustering));
			//clusterMaxArtistMappingDf.sort("cluster").show(275);
			//clusterMaxArtistMappingDf.sort("ArtistId").show(275);
			
			System.out.println("mapPopularArtistToUserCluster :ENDS -->" + printCurrentTime());
			return clusterMaxArtistMappingDf;
	}

	//Read Meta Data from locally or s3 bucket
	private Dataset<Row> readMetaDataOfSongs(SparkSession sparkSession) {
		
				Dataset<Row> metaDataDF = sparkSession.read().option("header", false).option("inferSchema",true)
										.csv(readFromFiles(sourceFolderOrBucket,songMetaDataEndPoint))
										.toDF(new String[] {"SongId","ArtistId"});
				metaDataDF = metaDataDF.select(metaDataDF.col("ArtistId").cast("long"),metaDataDF.col("SongId").cast("String")).na().drop();
				//metaDataDF.show();
				return metaDataDF;
	}

	private Dataset<Row> readUsersStreamActivity(SparkSession sparkSession) {
		System.out.println("readUsersStreamActivity :STARTS -->" + printCurrentTime());
		Dataset<Row> userActivityClickStreamDf = null;
		if (isConnetingS3) {
			String fileName = clckStreamEndPoint;
			
			userActivityClickStreamDf = sparkSession.read().option("header", false).option("inferSchema", true)
					.csv(fileName)
					.toDF(new String[] { "UserId", "TimeStamp", "SongId", "Date" }).drop("TimeStamp").drop("Date").na()
					.drop();
		} else {
			userActivityClickStreamDf = sparkSession.read().option("header", false).option("inferSchema", true)
					.csv(readFromFiles(sourceFolderOrBucket, clckStreamEndPoint))
					// Rename columns as required
					.toDF(new String[] { "UserId", "TimeStamp", "SongId", "Date" }).drop("TimeStamp").drop("Date").na()
					.drop();
		}


		// Indexing the UserId,SongID columns
		StringIndexer userIndexer = new StringIndexer().setInputCol("UserId").setOutputCol("UserIndex");
		StringIndexer songIndexer = new StringIndexer().setInputCol("SongId").setOutputCol("SongIndex");
		Pipeline indexUserPipeline = new Pipeline();
		indexUserPipeline.setStages(new PipelineStage[] { userIndexer, songIndexer });
		userActivityClickStreamDf = indexUserPipeline.fit(userActivityClickStreamDf)
				.transform(userActivityClickStreamDf);

	
		userActivityClickStreamDf = userActivityClickStreamDf
				.select(userActivityClickStreamDf.col("UserId"), userActivityClickStreamDf.col("SongId"),
						userActivityClickStreamDf.col("UserIndex").cast("Integer"),
						userActivityClickStreamDf.col("SongIndex").cast("Integer"))
				.groupBy("UserId", "SongId", "UserIndex", "SongIndex").count().withColumnRenamed("count", "hits").na()
				.drop();

		//userActivityClickStreamDf.show();

		// areaOfOperation("Analyze Implicit Relationship between users");
		ALSModel chosenModel = trainDataForALS(userActivityClickStreamDf);
		Dataset<Row> userFactorsDf = chosenModel.userFactors();

		// Factorize the features generated by ALS
		userFactorsDf = userFactorsDf.withColumn("feature_col_1", userFactorsDf.col("features").getItem(0))
				.withColumn("feature_col_2", userFactorsDf.col("features").getItem(1))
				.withColumn("feature_col_3", userFactorsDf.col("features").getItem(2))
				.withColumn("feature_col_4", userFactorsDf.col("features").getItem(3)).drop("features");
		// userFactorsDf.cache();
		//userFactorsDf.show();

		// Vectorize the factors generated by ALS
    	ArrayList<String> vectorInputColsList = new ArrayList<String>(Arrays.asList(userFactorsDf.columns()));
		String[] colsToVectorize = vectorInputColsList.toArray(new String[vectorInputColsList.size()]); 
		VectorAssembler assembler = new VectorAssembler().setInputCols(colsToVectorize).setOutputCol("features");
		Dataset<Row> userFactorVectorizedDf =  assembler.transform(userFactorsDf);		

		//userFactorVectorizedDf.show();

		Dataset<Row> userVectorScaledDf = scaleFactors(userFactorVectorizedDf);
		//userVectorScaledDf.show();

		
		  System.out.println("*************Clustering by KMeans**************************");
		  Dataset<Row> userClusteredDf = clusterByKMeans(userVectorScaledDf);
		  //userClusteredDf.show();
		  
		  System.out.println("*************Checking distribution of KMeans**************************");
		  userClusteredDf.groupBy("cluster").count().orderBy("cluster");//.show();
			
		  System.out.println("*************Maps Activity Df to Clusters generated*******************");
		  //userActivityClickStreamDf.show();
		  //userClusteredDf.show();
		  System.out.println(" *****trying broadcasting***  >" + printCurrentTime());
		  userActivityClickStreamDf = userActivityClickStreamDf.repartition();
			userActivityClickStreamDf = userActivityClickStreamDf.join(broadcast(userClusteredDf),"UserIndex").drop("features");//.withColumnRenamed("prediction", "cluster");
			//userActivityClickStreamDf.show();
			
		System.out.println("readUsersStreamActivity :ENDS -->" + printCurrentTime());
			return userActivityClickStreamDf;
	}

private Dataset<Row> clusterByKMeans(Dataset<Row> inputDataDf) {
		
		// Evaluate clustering by computing Silhouette score
		ClusteringEvaluator kMeansEvaluator = new ClusteringEvaluator();
		
		/***
		 * Block is commented for save time during analysis. It was looped over them between 
		 * 150 to 450 and found that the best value was around 250 
		 * Then looping was done between 250 upwards till 280 by stepping by 5 to determine the closest K as 275 was obtained
		KMeansModel chosenModel=null, kMeansmodel = null;
		double prevRateOfChangeOfSilhouette = -2, rateOfChangeOfSilhoutte = 0;
		double prevSilhoutte = 0;
		double minWSSSE = Double.MAX_VALUE;
		int chosenCluster = -1;
//		for(int noOfClusters=150;noOfClusters <= 450 ;noOfClusters+=100) {
		for(int noOfClusters=250;noOfClusters <= 280 ;noOfClusters+=4) {
			areaOfOperation("Running KMeans For Cluster Size " + noOfClusters);

			KMeans kMeans = new KMeans().setK(noOfClusters).setSeed(1L);
			kMeansmodel = kMeans.fit(inputDataDf);
	
			// Make predictions
			Dataset<Row> predictions = kMeansmodel.transform(inputDataDf);
			double Silhouette = kMeansEvaluator.evaluate(predictions);
			double WSSSE = kMeansmodel.computeCost(inputDataDf);
			if(prevRateOfChangeOfSilhouette != -2) 
				rateOfChangeOfSilhoutte = (Silhouette - prevSilhoutte) / prevSilhoutte;
			System.out.println("Silhouette with squared euclidean distance = " + Silhouette);
			System.out.println("Within Set Sum of Squared Errors  = " + WSSSE);
			System.out.println("Rate of Change of Silhouette = " + rateOfChangeOfSilhoutte*100 + "%");
			prevSilhoutte = Silhouette;
			if(prevRateOfChangeOfSilhouette == -2 || (rateOfChangeOfSilhoutte > prevRateOfChangeOfSilhouette && minWSSSE > WSSSE)) {
				System.out.println("Chosen cluster as per current Iteration is :---> " + noOfClusters);
				prevRateOfChangeOfSilhouette = rateOfChangeOfSilhoutte;
				minWSSSE = WSSSE;
				chosenModel = kMeansmodel;
				chosenCluster = noOfClusters;
			}
		}
		
		*/
		int chosenCluster = 275;
		KMeans kMeans = new KMeans().setK(chosenCluster);
		KMeansModel chosenModel = kMeans.fit(inputDataDf);
		Dataset<Row> predictResults = chosenModel.transform(inputDataDf);
		double Silhouette = kMeansEvaluator.evaluate(predictResults);
		double WSSSE = chosenModel.computeCost(inputDataDf);
		System.out.println("Final k chosen for Clusters:  ---> " + chosenCluster);
		System.out.println("Chosen Cluster Silhouette  :---> " + Silhouette);
		System.out.println("Chosen Cluster Within Set Sum of Squared Errors:---> " + WSSSE);
		return predictResults.withColumnRenamed("prediction", "cluster").withColumnRenamed("id", "UserIndex");
	}
	private ALSModel trainDataForALS(Dataset<Row> inputData) {
		System.out.println("trainDataForALS :STARTS -->" + printCurrentTime());
		printCurrentTime();
		ALS trainingALS = new ALS();
		trainingALS.setMaxIter(8);
		trainingALS.setRegParam(0.01);
		trainingALS.setUserCol("UserIndex");
		trainingALS.setItemCol("SongIndex");
		trainingALS.setRatingCol("hits");
		trainingALS.setImplicitPrefs(true);
		// trainingALS.setAlpha(10.00); -- decided not required as this is did not
		// improve the cluster
		trainingALS.setRank(4);
		ALSModel chosenModel = trainingALS.fit(inputData);
		printCurrentTime();
		System.out.println("trainDataForALS :ENDS -->" + printCurrentTime());
		return chosenModel;
	}

	/**
	 * Scaling the factors formed by implicit analysis.
	 * 
	 * @param inputFactorVectorDf
	 * @return
	 */
	private Dataset<Row> scaleFactors(Dataset<Row> inputFactorVectorDf) {
		System.out.println("scaleFactors :STARTS -->" + printCurrentTime());
		printCurrentTime();
		StandardScaler scaler = new StandardScaler().setInputCol("features").setOutputCol("scaledFeatures")
				.setWithStd(true).setWithMean(true);
		System.out.println("scaleFactors :ENDS -->" + printCurrentTime());
		return scaler.fit(inputFactorVectorDf).transform(inputFactorVectorDf).select("id", "scaledFeatures")
				.withColumnRenamed("scaledFeatures", "features");
	}

	/**
	 * Vectorize the factors
	 * 
	 * @param inputDf
	 * @param idColumnToDrop
	 * @return
	 */
	private Dataset<Row> vectorizeingFactors(Dataset<Row> inputDf) {
		System.out.println("vectorizeingFactors :STARTS ->" + printCurrentTime());
		ArrayList<String> vectorInputColsList = new ArrayList<String>(Arrays.asList(inputDf.columns()));
		String[] colsToVectorize = vectorInputColsList.toArray(new String[vectorInputColsList.size()]); // .parallelStream().toArray(String[]::new);
		VectorAssembler assembler = new VectorAssembler().setInputCols(colsToVectorize).setOutputCol("features");
		System.out.println("vectorizeingFactors :ENDS");
		return assembler.transform(inputDf);
	}

	private String[] readFromFiles(String sourceFolderorBucket, String subFolderorFolderKey) {
		if (isConnetingS3) {
			 return new String[] {"s3a://" + sourceFolderorBucket + "/" + subFolderorFolderKey + "/*"};
			//return fetchDataFromS3(sourceFolderorBucket, subFolderorFolderKey);
		} else {
			return getLocalFilesList(sourceFolderorBucket + subFolderorFolderKey);
		}
	}

	private String[] fetchDataFromS3(String bucketName, String folderKey) {

		// Dataset<Row> clckStreamDF = spark.read().option("header",
		// false).option("inferschema", true).csv(clckStreamEndPoint);
		Regions clientRegion = Regions.US_EAST_1;
		List<String> keys = new ArrayList<>();

		try {

			BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);

			AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(clientRegion)
					.withCredentials(new AWSStaticCredentialsProvider(credentials)).build();

			ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName)
					.withPrefix(folderKey + "/");
			ObjectListing objects = s3Client.listObjects(listObjectsRequest);
			do {
				List<S3ObjectSummary> summaries = objects.getObjectSummaries();
				if (summaries.size() < 1) {
					break;
				}
				summaries.forEach(s -> keys.add("s3a://" + bucketName + "/" + s.getKey()));
				objects = s3Client.listNextBatchOfObjects(objects);
			} while (!objects.isTruncated());
		} catch (AmazonServiceException e) {
			// The call was transmitted successfully, but Amazon S3 couldn't process
			// it, so it returned an error response.
			e.printStackTrace();
		} catch (SdkClientException e) {
			// Amazon S3 couldn't be contacted for a response, or the client
			// couldn't parse the response from Amazon S3.
			e.printStackTrace();
		}
		return keys.toArray(new String[keys.size()]);
	}

	// Loop through the directory and return list of file names or the file name if
	// it is not directory
	private String[] getLocalFilesList(String directoryName) {
		File file = new File(directoryName);
		String[] listFiles = file.list();
		if (listFiles != null) {
			for (int i = 0; i < listFiles.length; i++)
				listFiles[i] = directoryName + listFiles[i];
			return listFiles;
		} else
			return new String[] { directoryName };
	}

	private Dataset<Row> doAnalysis(Dataset<Row> notificationsToArtistClusterUsersMappingDF, Dataset<Row> notificiationsClicksToClusterMappingDF) {
		System.out.println("doAnalysis :STARTS -->" + printCurrentTime());
		System.out.println("**********Cluster based Notification Clicks*******");
		// Group notification clicks to get the count of clicks by cluster, notification
		Dataset<Row> clusterNotificationClicks = notificiationsClicksToClusterMappingDF.groupBy("NotificationId","ArtistId","artistCluster")
										  .agg(org.apache.spark.sql.functions.countDistinct("UserId"))
										  .withColumnRenamed("count(DISTINCT UserId)", "clusterUserClicks");
		//clusterNotificationClicks.show();
		
		System.out.println("*********Cluster based Notification Total**************");
		// Group notification mapping to the count of notifications by cluster, notification
		Dataset<Row> clusterNotificationTotal = notificationsToArtistClusterUsersMappingDF.groupBy("NotificationId","ArtistId","artistCluster")
											  .agg(org.apache.spark.sql.functions.countDistinct("UserId"))
											  .withColumnRenamed("count(DISTINCT UserId)", "clusterUserCounts");
		
		//clusterNotificationTotal.cache();
        //clusterNotificationTotal.show();

		//Merge to find the total clicks vs count for notification by cluster
        System.out.println("**********Notification Clicks and Notification Total --- By Cluster*****************");
		Dataset<Row> notificationTrendsDF = clusterNotificationClicks.join(clusterNotificationTotal,"NotificationId")
											.select(clusterNotificationClicks.col("NotificationId"),
														clusterNotificationClicks.col("clusterUserClicks"),
														clusterNotificationTotal.col("clusterUserCounts"));
		//notificationTrendsDF.show();

		//Summarize total clicks vs count for notification by dropping cluster
		 System.out.println("***********Notification Clicks and Notification Total --- Across Cluster Roll up for Artist**********");
		notificationTrendsDF = notificationTrendsDF.groupBy("NotificationId")
							.agg(org.apache.spark.sql.functions.sum("clusterUserClicks"),org.apache.spark.sql.functions.sum("clusterUserCounts"))
							.withColumnRenamed("sum(clusterUserClicks)", "notificationClicks")
							.withColumnRenamed("sum(clusterUserCounts)", "notificationTotalCounts");
		//notificationTrendsDF.show();

		//Compute click through rate as clicks / count from previous summary
		System.out.println("********* Notification - CTR Trend Top ********");
		WindowSpec notificationOrder = org.apache.spark.sql.expressions.Window.orderBy(notificationTrendsDF.col("notificationClicks").desc());
		Column ctrColumn = (notificationTrendsDF.col("notificationClicks").divide(notificationTrendsDF.col("notificationTotalCounts"))).multiply(100);
		notificationTrendsDF = notificationTrendsDF.withColumn("CTR", ctrColumn)
								.withColumn("rowNumber", org.apache.spark.sql.functions.row_number().over(notificationOrder))
								.where("rowNumber <= 5");
		notificationTrendsDF.show();
		System.out.println("doAnalysis :ENDS -->" + printCurrentTime());
		return notificationTrendsDF;
	}
	
	private String printCurrentTime() {
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
		LocalDateTime now = LocalDateTime.now();
		return dtf.format(now);
	}
	/**
	 * File Operation 
	 */

	public void writeDataFrame(Dataset<Row> inputDataset,int type,SparkSession sparkSession,String targetfileName) {
		SparkContext sc = sparkSession.sparkContext();
		inputDataset = inputDataset.limit(10000); // decreasing the dataset size to improve file I/O operation
		try {
			FileSystem fs = FileSystem.get(sc.hadoopConfiguration());
			String fileName = "";
			switch(type) {
				//Process for UserClusterCombinationDetails
				case 0 :
					inputDataset.coalesce(1).write().mode(SaveMode.Overwrite).format("csv").option("header", true).save("saavn/users/"+targetfileName+".csv");
					//fileName = fs.globStatus(new Path("saavn/users/"+"part*"))[0].getPath().getName();
					//fs.rename(new Path("saavn/users/" + fileName), new Path("saavn/users/" + targetfileName  + ".csv"));
					break;
				case 1:
					//Process for ArtistID and UserID of top5 notifications 
					//inputDataset.repartition(col("NotificationId")).write().mode(SaveMode.Overwrite).format("csv").option("headers", true).save("saavn/notifications/");
					//inputDataset.lim
					inputDataset.write().partitionBy("NotificationId").mode(SaveMode.Overwrite).format("csv").option("headers", true).save("saavn/notifications/");
					/*FileStatus[] fileStatus  =  fs.listStatus(new Path("saavn/notifications"));
					 for (FileStatus fileStat : fileStatus) {
					        if (fileStat.isDirectory()) {
					            String filePath = fileStat.getPath().toString();
					            int equalToIndex = filePath.lastIndexOf("NotificationId=");
					            if(equalToIndex != -1)
					            {
					            	targetfileName  = filePath.substring(equalToIndex+1);
									fs.rename(new Path(filePath), new Path("saavn/notifications/" + targetfileName  + ".csv"));
					            } else 
					            break;
					        }
					    }*/
					break;
				case 2:
					//Process for top5 notifications details
					inputDataset.coalesce(1).write().mode(SaveMode.Overwrite).format("csv").option("header", true).save("saavn/top5/" + targetfileName  + ".csv");
					//fileName = fs.globStatus(new Path("saavn/users/"+"part*"))[0].getPath().getName();
					//fs.rename(new Path("saavn/users/" + fileName), new Path("saavn/top5/" + targetfileName  + ".csv"));
					break;
				default:
					break;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}
