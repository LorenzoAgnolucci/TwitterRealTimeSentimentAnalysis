import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.ArrayList;

public class Utils{
    public static Writable toWritable(ArrayList<String> list) {
        Writable[] content = new Writable[list.size()];
        for (int i = 0; i < content.length; i++) {
            content[i] = new Text(list.get(i));
        }
        return new ArrayWritable(Text.class, content);
    }
    public static ArrayList<String> fromWritable(ArrayWritable writable) {
        Writable[] writables = ((ArrayWritable) writable).get();
        ArrayList<String> list = new ArrayList<String>(writables.length);
        for (Writable wrt : writables) {
            list.add(((Text)wrt).toString());
        }
        return list;
    }
}
