package nearsoft.academy.bigdata.recommendation;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class MovieRecommender {
    private final String path;
    private GenericUserBasedRecommender recommender;
    private int totalReviews;
    private HashBiMap<String, Integer> users;
    private HashBiMap<String, Integer> products;


    public MovieRecommender(String path) throws IOException, TasteException {
        this.path = path;
        fillData();
    }

    public int getTotalReviews() {
        return totalReviews;
    }

    public int getTotalProducts() {
        return products.size();
    }

    public int getTotalUsers() {
        return users.size();
    }

    public List<String> getRecommendationsForUser(String user) throws TasteException {
        List<RecommendedItem> recommendations = recommender.recommend(users.get(user), 3);
        List<String> recommendedMovies = new ArrayList();
        BiMap<Integer, String> productsByName = products.inverse();
        for (RecommendedItem recommendation : recommendations) {
            String movieName = productsByName.get((int) recommendation.getItemID());
            recommendedMovies.add(movieName);
        }


        return recommendedMovies;
    }

    private void fillData() throws IOException, TasteException {
        totalReviews = 0;
        users = HashBiMap.create();
        products = HashBiMap.create();

        File movies = new File("movies.csv");
        if (movies.exists()) {
            movies.delete();
        } else {
            movies.createNewFile();
        }
        InputStream fileStream = new FileInputStream(path);
        InputStream gzipStream = new GZIPInputStream(fileStream);
        Reader decoder = new InputStreamReader(gzipStream, "UTF8");
        BufferedReader buffered = new BufferedReader(decoder);
        Writer writer = new BufferedWriter(new FileWriter(movies));
        String sub;
        Integer idUser = null;
        String score = "";
        Integer idProduct = null;
        boolean newElement = false;
        while ((sub = buffered.readLine()) != null) {
            if (newElement) {
                if (sub.contains("review/userId")) {
                    String userName = sub.substring(sub.indexOf(":") + 2, sub.length());
                    if (!users.containsKey(userName)) {
                        users.put(userName, users.size() + 1);
                    }
                    idUser = users.get(userName);
                } else if (sub.contains("review/score")) {
                    score = sub.substring(sub.indexOf(":") + 2, sub.length());
                } else if (sub.contains("review/summary")) {
                    writer.append(String.valueOf(idUser));
                    writer.append(",");
                    writer.append(String.valueOf(idProduct));
                    writer.append(",");
                    writer.append(score);
                    writer.append("\n");

                    idUser = null;
                    score = "";
                    idProduct = null;
                    newElement = false;
                }
            } else if (sub.contains("product/productId")) {
                String productName = sub.substring(sub.indexOf(":") + 2, sub.length());
                if (!products.containsKey(productName)) {
                    products.put(productName, products.size() + 1);
                }
                idProduct = products.get(productName);
                newElement = true;
                totalReviews++;
            }
        }
        writer.close();
        DataModel dataModel = new FileDataModel(new File("movies.csv"));
        UserSimilarity similarity = new PearsonCorrelationSimilarity(dataModel);
        UserNeighborhood neighborhood = new ThresholdUserNeighborhood(0.1, similarity, dataModel);
        recommender = new GenericUserBasedRecommender(dataModel, neighborhood, similarity);
    }
}
