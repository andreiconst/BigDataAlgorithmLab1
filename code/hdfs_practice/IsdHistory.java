package ecp.lab1;


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class IsdHistory {
    public static void main(String[] args) throws FileNotFoundException, IOException {

        String line = null;
        Integer count = 0;
        
        BufferedReader read = new BufferedReader(new FileReader("/home/cloudera/Desktop/Lab1/Data/isd-history.txt"));
        while ((line = read.readLine()) != null) {

            // use comma as separator
            if (count >= 22){
            	String usaf = line.substring(0, 6);
            	String station = line.substring(13, 42);
            	String country = line.substring(43, 45);
            	String altitude = line.substring(74, 81);
            	
            	if(count <= 122){
            	
            	System.out.println("USAF: " + usaf + "; station: " + station + "; country: " + country + "; elevation: " + altitude);
            	
            	}
            }
            	
            count = count + 1;

        }
        read.close();

    }

}
