# Song-Recommendation-System
Implemented using Hadoop MapReduce 

Implemented using Hadoop MapReduce on the Yahoo! Music user ratings of songs with song attributes, version 1.0 (R2) dataset from the Webscope Datasets available from Yahoo! Labs that contains over 70 million records.The program uses item-based collaborative filtering to recommend a list of top 10 songs to the user. The songs are chosen based on similarity of songs which user has listened to/liked previously, and a prediction measure is calculated for the user and similar songs. 

It involves following tasks:
1. Preprocessing: Representing training data in a form that is suitable to be used by next task.
2. Similarity Calculation: Calculate similarity measure for each pair of songs in the input. Similarity is calculated using four measures: Pearson coefficient, Cosine similarity, Jaccard similarity for non-binary vectors & Euclidean similarity. In each case, their running time are measured and compared.
We get a list of songs similar to a given song.
3. Prediction: In the test data, compute predicted rating for each user and then calculate the accuracy using Mean Absolute Error method.
4. Recommendation: Compute top 10 recommendation, based on the predicted rating for item i similar item list ofitem j which is already rated by user u.
