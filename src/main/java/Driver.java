import java.io.IOException;

/**
 * Created by zzc on 8/4/17.
 */
public class Driver {
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        String rawData = args[0];
        String userMovieListOutputDir = args[1];
        String coCurrenceMatrixOutputDir = args[2];

        String[] path1 = {rawData, userMovieListOutputDir};
        String[] path2 = {userMovieListOutputDir, coCurrenceMatrixOutputDir};

        DataDividedByUser dataDividedByUser = new DataDividedByUser();
        dataDividedByUser.main(path1);

        CoCurrenceMatrix coCurrenceMatrix = new CoCurrenceMatrix();
        coCurrenceMatrix.main(path2);
    }
}
