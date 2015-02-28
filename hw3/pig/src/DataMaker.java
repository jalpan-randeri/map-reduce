import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.lang.Exception;

public class DataMaker{

    public static void main(String[] args) throws Exception{
        BufferedReader reader = new BufferedReader(new FileReader("/Users/jalpanranderi/Downloads/data.csv"));
        FileWriter writer = new FileWriter("test.csv");
        int count = 1000;
        String line;
        boolean flag = false;
        while((line = reader.readLine())!=null){

            if(flag) {
                writer.write(line);
                writer.write("\n");
                count--;
            }

            if(count == 0){
                break;
            }


            String[] l = line.split(",");
            if(l[0].equals("2007") && l[2].equals("12")){
                System.out.println(count);
                flag = true;
            }

        }

        reader.close();
        writer.close();
    }
}