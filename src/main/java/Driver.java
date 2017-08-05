/**
 * Created by zzc on 8/4/17.
 */
public class Driver {
    public static void main(String args[]) {
        String rawData = args[0];
        String userMovieListOutputDir = args[1];
        String[] path1 = {rawData, userMovieListOutputDir};

        DataDividedByUser dataDividedByUser = new DataDividedByUser();
        dataDividedByUser.main(path1);
    }
}
