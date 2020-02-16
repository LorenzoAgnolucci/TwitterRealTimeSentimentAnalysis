import javafx.animation.Animation;
import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.beans.property.SimpleStringProperty;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.chart.BarChart;
import javafx.scene.chart.CategoryAxis;
import javafx.scene.chart.NumberAxis;
import javafx.scene.chart.XYChart;
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.util.Duration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.ResourceBundle;

public class PresentationLayerController implements Initializable{

    @FXML
    private TableView<String[]> realTimeTableView;
    @FXML
    private TableColumn<String[], String> realTimeKeywordColumn;
    @FXML
    private TableColumn<String[], String> realTimePositiveColumn;
    @FXML
    private TableColumn<String[], String> realTimeNegativeColumn;

    @FXML
    private TableView<String[]> batchTableView;
    @FXML
    private TableColumn<String[], String> batchKeywordColumn;
    @FXML
    private TableColumn<String[], String> batchPositiveColumn;
    @FXML
    private TableColumn<String[], String> batchNegativeColumn;

    @FXML
    private BarChart<String, Number> combinedViewsChart;

    private Table realTimeViewTable;
    private Table batchViewTable;

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle){
        try{
            Configuration conf = HBaseConfiguration.create();
            Connection connection = ConnectionFactory.createConnection(conf);
            this.realTimeViewTable = connection.getTable(TableName.valueOf("tweet_realtime_database"));
            this.batchViewTable = connection.getTable(TableName.valueOf("tweet_batch_view"));


            //Real Time TableView

            realTimeTableView.setPlaceholder(new Label("Empty Table"));

            realTimeKeywordColumn.setCellValueFactory(p -> {
                String[] row = p.getValue();
                return new SimpleStringProperty(row[0]);
            });
            realTimePositiveColumn.setCellValueFactory(p -> {
                String[] row = p.getValue();
                return new SimpleStringProperty(row[1]);
            });
            realTimeNegativeColumn.setCellValueFactory(p -> {
                String[] row = p.getValue();
                return new SimpleStringProperty(row[2]);
            });

            realTimeTableView.getItems().setAll(parseRealTimeResultsList(realTimeViewTable));


            //Batch TableView

            batchTableView.setPlaceholder(new Label("Empty Table"));

            batchKeywordColumn.setCellValueFactory(p -> {
                String[] row = p.getValue();
                return new SimpleStringProperty(row[0]);
            });
            batchPositiveColumn.setCellValueFactory(p -> {
                String[] row = p.getValue();
                return new SimpleStringProperty(row[1]);
            });
            batchNegativeColumn.setCellValueFactory(p -> {
                String[] row = p.getValue();
                return new SimpleStringProperty(row[2]);
            });

            batchTableView.getItems().setAll(parseBatchResultsList(batchViewTable));


            //Bar Chart

            XYChart.Series<String, Number> positiveSeries = new XYChart.Series<String, Number>();
            positiveSeries.setName("Positive");
            XYChart.Series<String, Number> negativeSeries = new XYChart.Series<String, Number>();
            negativeSeries.setName("Negative");

            for(String[] row: parseChartSeries(batchViewTable, realTimeViewTable)){
                positiveSeries.getData().add(new XYChart.Data<String, Number>(row[0], Integer.parseInt(row[1])));
                negativeSeries.getData().add(new XYChart.Data<String, Number>(row[0], Integer.parseInt(row[2])));
            }
            combinedViewsChart.getData().addAll(positiveSeries, negativeSeries);


            //Refresh TableViews and Chart with the new data

            Timeline clock = new Timeline(new KeyFrame(Duration.ZERO, e -> {
                try{
                    realTimeTableView.getItems().setAll(parseRealTimeResultsList(realTimeViewTable));
                    batchTableView.getItems().setAll(parseBatchResultsList(batchViewTable));
                    realTimeTableView.refresh();
                    batchTableView.refresh();

                    positiveSeries.getData().clear();
                    negativeSeries.getData().clear();
                    combinedViewsChart.setAnimated(false);
                    for(String[] row: parseChartSeries(batchViewTable, realTimeViewTable)){
                        positiveSeries.getData().add(new XYChart.Data<String, Number>(row[0], Integer.parseInt(row[1])));
                        negativeSeries.getData().add(new XYChart.Data<String, Number>(row[0], Integer.parseInt(row[2])));
                    }
                    combinedViewsChart.getData().clear();
                    combinedViewsChart.getData().addAll(positiveSeries, negativeSeries);
                }catch(IOException ex){
                    ex.printStackTrace();
                }
            }),
                    new KeyFrame(Duration.seconds(3))
            );
            clock.setCycleCount(Animation.INDEFINITE);
            clock.play();

        }catch(IOException e){
            e.printStackTrace();
        }

    }

    private ArrayList<String[]> parseBatchResultsList(Table table) throws IOException{
        ArrayList<String[]> resultList = new ArrayList<>();

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("sentiment_count"));
        ResultScanner resultScanner = table.getScanner(scan);
        for(Result result = resultScanner.next(); result != null; result = resultScanner.next()){
            String rowKey = Bytes.toString(result.getRow());
            String positiveCount = Bytes.toString(result.getValue(Bytes.toBytes("sentiment_count"), Bytes.toBytes("positive")));
            String negativeCount = Bytes.toString(result.getValue(Bytes.toBytes("sentiment_count"), Bytes.toBytes("negative")));

            negativeCount = negativeCount != null ? negativeCount : "0";
            positiveCount = positiveCount != null ? positiveCount : "0";

            resultList.add(new String[]{rowKey, positiveCount, negativeCount});
        }
        return resultList;
    }

    private ArrayList<String[]> parseRealTimeResultsList(Table table) throws IOException{
        ArrayList<String[]> resultList = new ArrayList<>();

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("content"));
        ResultScanner resultScanner = table.getScanner(scan);
        ArrayList<String> keywords = new ArrayList<>();
        ArrayList<Integer> positiveCount = new ArrayList<>();
        ArrayList<Integer> negativeCount = new ArrayList<>();
        for(Result result = resultScanner.next(); result != null; result = resultScanner.next()){
            String keyword = Bytes.toString(result.getValue(Bytes.toBytes("content"), Bytes.toBytes("keyword")));
            String sentimentString = Bytes.toString(result.getValue(Bytes.toBytes("content"), Bytes.toBytes("sentiment")));
            int sentiment = Integer.parseInt(sentimentString);

            if(keywords.contains(keyword)){
                int index = keywords.indexOf(keyword);
                if(sentiment == 1){
                    positiveCount.set(index, positiveCount.get(index) + 1);
                }
                else{
                    negativeCount.set(index, negativeCount.get(index) + 1);
                }
            }
            else{
                keywords.add(keyword);
                if(sentiment == 1){
                    positiveCount.add(1);
                    negativeCount.add(0);
                }
                else{
                    positiveCount.add(0);
                    negativeCount.add(1);
                }
            }
        }

        for(int i = 0; i < keywords.size(); i++){
            resultList.add(new String[]{keywords.get(i), positiveCount.get(i).toString(), negativeCount.get(i).toString()});
        }
        return resultList;
    }

    private ArrayList<String[]> parseChartSeries(Table batchTable, Table realTimeTable) throws IOException{
        ArrayList<String[]> chartSeries = new ArrayList<>();
        ArrayList<String[]> batchResults = parseBatchResultsList(batchTable);
        ArrayList<String[]> realTimeResults = parseRealTimeResultsList(realTimeTable);
        for(String[] batchRow: batchResults){
            int positiveCount = Integer.parseInt(batchRow[1]);
            int negativeCount = Integer.parseInt(batchRow[2]);
            for(String[] realTimeRow: realTimeResults){
                if(batchRow[0].equals(realTimeRow[0])){
                    positiveCount += Integer.parseInt(realTimeRow[1]);
                    negativeCount += Integer.parseInt(realTimeRow[2]);
                }
            }
            chartSeries.add(new String[]{batchRow[0], String.valueOf(positiveCount), String.valueOf(negativeCount)});
        }
        if(realTimeResults.size() > chartSeries.size()){
            for(String[] realTimeRow : realTimeResults){
                boolean found = false;
                for(String[] chartRow: chartSeries){
                    found = realTimeRow[0].equals(chartRow[0]);
                }
                if(!found){
                    chartSeries.add(realTimeRow);
                }
            }
        }
        return chartSeries;
    }
}