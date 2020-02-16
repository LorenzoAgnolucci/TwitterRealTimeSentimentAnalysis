import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;

import java.io.IOException;

public class PresentationLayer extends Application {

    private static Scene scene;

    @Override
    public void start(Stage stage) throws IOException{
        scene = new Scene(loadFXML("GUI_Presentation_Layer"));
        stage.setTitle("Twitter Real Time Sentiment Analysis");
        scene.getStylesheets().add(PresentationLayer.class.getResource("Style.css").toExternalForm());
        stage.setScene(scene);
        stage.show();
    }

    private static Parent loadFXML(String fxml) throws IOException {
        FXMLLoader fxmlLoader = new FXMLLoader(PresentationLayer.class.getResource(fxml + ".fxml"));
        return fxmlLoader.load();
    }

    public static void main(String[] args) {
        launch();
    }

}