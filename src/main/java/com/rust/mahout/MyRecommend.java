 /*
  * Package com.rust.mahout
  * FileName: MyRecommender
  * Author:   Takho
  * Date:     19/3/10 14:17
  */
 package com.rust.mahout;

 import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
 import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
 import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
 import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
 import org.apache.mahout.cf.taste.model.DataModel;
 import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
 import org.apache.mahout.cf.taste.recommender.RecommendedItem;
 import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
 import org.apache.mahout.cf.taste.similarity.UserSimilarity;

 import java.io.File;
 import java.util.List;

 /**
  * 推荐
  *
  * @author Takho
  */
 public class MyRecommend {
	 public static void main(String[] args) throws Exception {
		 // 创建数据模型
		 DataModel dataModel = new FileDataModel(new File(MyRecommend.class.getResource("/a.txt").toURI()));

		 // 创建UserSimilarity对象
		 UserSimilarity similarity = new PearsonCorrelationSimilarity(dataModel);

		 // 创建UserNeighborhood对象
		 UserNeighborhood neighborhood = new ThresholdUserNeighborhood(1.0, similarity, dataModel);
		 // 创建推荐器对象
		 UserBasedRecommender recommender = new GenericUserBasedRecommender(dataModel, neighborhood, similarity);

		 //	向B用户推荐3个产品
		 List<RecommendedItem> recommendations = recommender.recommend(2, 3);

		 for (RecommendedItem recommendation : recommendations) {
			 System.out.println(recommendation);
		 }


	 }
 }
