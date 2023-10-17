package evaluation;

import util.Constants;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class Project{

    /**
    * This method returns the list of suffixes used for storing the different project's assessment.
    * The indexes for a specific project have this suffixes concatenated to the name of the index.
    *
    * @return The list of suffixes (strings) corresponding to the metrics indexes.
    */
    public static List<String> getProjects() {
        List<String> collections = util.Queries.getCollections();
        List<String> result = new ArrayList<>();
        int pos_index, pos_suffix;
        boolean is_added;

        for (String collection : collections) {
            is_added = false;
            pos_index = collection.lastIndexOf(Constants.INDEX_METRICS);
            if (pos_index != -1) {
                pos_suffix = collection.indexOf('.', pos_index);
                if (pos_suffix != -1) {
                    String cmp = collection.substring(pos_suffix + 1);
                    result.add(cmp);
                    System.err.println("GETPROJECT: index addex: " + collection + " (project: " + cmp + ")");
                    is_added = true;
                }
            }
            if (!is_added) System.err.println("GETPROJECT: index DON'T addex: " + collection);
        }
        return result;
    }

    private static String convertStreamToString(InputStream is) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        StringBuilder sb = new StringBuilder();
        String line;
        try {
            while ((line = reader.readLine()) != null)
                sb.append(line).append("\n");
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            try {
                is.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return sb.toString();
    }
}
