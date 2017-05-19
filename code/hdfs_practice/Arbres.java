package ecp.lab1;


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class Arbres {
    public static void main(String[] args) throws FileNotFoundException, IOException {

        String line = null;
        
        BufferedReader read = new BufferedReader(new FileReader("/home/cloudera/Desktop/Lab1/Data/arbres.csv"));
        while ((line = read.readLine()) != null) {

            // use comma as separator
            String[] arbreslist = line.split(";");
            
            if((!arbreslist[5].isEmpty()) && (!arbreslist[6].isEmpty())){

            System.out.println("Year: " + arbreslist[5] + ", height: " + arbreslist[6]);
            
            }

        }
        read.close();

    }

}
