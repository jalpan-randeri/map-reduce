import sun.jvm.hotspot.bugspot.BugSpot;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * Created by jalpanranderi on 2/24/15.
 */
public class Test {

    static Date START_DATE;
    static Date END_DATE;



    public static void main(String[] args) throws IOException, ParseException {
        average();



    }





    private static boolean isFlightInSpecifiedInterval(String date) throws ParseException {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Date d = sdf.parse(date);
            return isWithinRange(d);

    }


    private static boolean isWithinRange(Date testDate) {
        return !(testDate.before(START_DATE) || testDate.after(END_DATE));
    }





    private static void average() throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader("output/part-r-00000"));
        String line = null;
        int count = 0;
        double sum = 0;
        while((line = reader.readLine())!= null){
            count++;
            sum = sum + extractSum(line);
        }

        System.out.println(count);
        System.out.println(sum/count);
    }

    private static double extractSum(String line) {
        return Double.parseDouble(line.split("\t")[1]);
    }
}
