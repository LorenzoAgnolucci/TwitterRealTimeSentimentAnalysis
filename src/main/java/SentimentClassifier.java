import au.com.bytecode.opencsv.CSVReader;

import com.aliasi.classify.Classification;
import com.aliasi.classify.Classified;
import com.aliasi.classify.DynamicLMClassifier;
import com.aliasi.classify.LMClassifier;
import com.aliasi.lm.LanguageModel;
import com.aliasi.lm.NGramProcessLM;
import com.aliasi.stats.MultivariateDistribution;
import com.aliasi.util.AbstractExternalizable;
import com.aliasi.util.CommaSeparatedValues;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class SentimentClassifier{

    public static final String TRAINING_FILE = "datasets/sentiment140/training.1600000.processed.noemoticon.csv";
    public static final String TEST_FILE = "datasets/twitter-sentiment-dataset-master/full-corpus.csv";
    public static final String MODEL_FILE = "SentimentClassifierTrainedModel.model";



    private DynamicLMClassifier<NGramProcessLM> trainingClassifier;
    private LMClassifier<LanguageModel, MultivariateDistribution> trainedClassifier;

    SentimentClassifier() {
        trainingClassifier = DynamicLMClassifier.createNGramProcess(new String[]{"1", "0"}, 8);
    }

    SentimentClassifier(File modelFile) throws IOException, ClassNotFoundException{
        trainedClassifier = (LMClassifier<LanguageModel, MultivariateDistribution>) AbstractExternalizable.readObject(modelFile);
    }

    void train(String fileName) throws IOException {
        System.out.println("Training classifier");
        File file = new File(fileName);
        CommaSeparatedValues csv = new CommaSeparatedValues(file, "UTF-8");
        String[][] rows = csv.getArray();

        int i = 0;
        for(String[] row : rows){
            if(row.length != 6){
                continue;
            }
            i++;
            System.out.println("Training on row: " + i);
            String text = row[5];
            String sentiment = row[0];
            if(sentiment.equals("4")){
                sentiment = "1";
            }
            Classification classification = new Classification(sentiment);
            Classified<CharSequence> classified = new Classified<CharSequence>(text, classification);
            trainingClassifier.handle(classified);
        }
    }

    void evaluate(String fileName) throws IOException {
        System.out.println("\nEvaluating classifier");

        List<List<String>> records = new ArrayList<>();
        try (CSVReader csvReader = new CSVReader(new FileReader(TEST_FILE));) {
            String[] values;
            while ((values = csvReader.readNext()) != null) {
                records.add(Arrays.asList(values));
            }
        }
        records.remove(0);
        int numTests = 0;
        int numCorrect = 0;
        for(List<String> row: records){
            if(row.get(1).equals("irrelevant") || row.get(1).equals("neutral")){
                continue;
            }
            numTests++;
            String text = row.get(4);
            String sentiment = row.get(1);
            if(sentiment.equals("positive")){
                sentiment = "1";
            }
            else{
                sentiment = "0";
            }

            Classification classification = trainedClassifier.classify(text);
            if (classification.bestCategory().equals(sentiment))
                ++numCorrect;
        }
        System.out.println("  # Test Cases=" + numTests);
        System.out.println("  # Correct=" + numCorrect);
        System.out.println("  % Correct=" + ((double)numCorrect)/(double)numTests);
    }

    public void storeModel(String fileName) throws IOException {
        FileOutputStream fileOutputStream = new FileOutputStream(fileName);
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
        trainingClassifier.compileTo(objectOutputStream);
        objectOutputStream.close();
    }

    public String classify(String tweet) {
        return trainedClassifier.classify(tweet).bestCategory();
    }

    public static void main(String[] args) throws Exception{
        File modelFile = new File(MODEL_FILE);
        if(modelFile.exists()){
            SentimentClassifier classifier = new SentimentClassifier(modelFile);
            classifier.evaluate(TEST_FILE);
        }
        else{
            SentimentClassifier classifier = new SentimentClassifier();
            classifier.train(TRAINING_FILE);
            classifier.storeModel(MODEL_FILE);
            System.out.println("Stored trained model in " + MODEL_FILE);
        }
    }
}
