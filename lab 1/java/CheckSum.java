import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;


public class CheckSum {

	 public static void main ( String [] args ) throws Exception {       
	        // The system configuration 
	   // creates a configuration object to configure the Hadoop system 
	        Configuration conf = new Configuration();  
	 
	        // Get an instance of the Filesystem using configuration 
	        FileSystem fs = FileSystem.get(conf); 
	         
	   // The path to the file that needs to be written 
	        String path_name = "/lab1/bigdata"; 
	         
	        Path path = new Path(path_name); 
	         
	        // The Output Data Stream to write into 
	        FSDataInputStream file = fs.open(path); 
	        byte [] buffer = new byte [1000];
	        long byteOffset = 1000000000;
	        int byteLen = 1000;
	        file.read(byteOffset, buffer, 0, byteLen);
	        int output = 0;
	    
	       
	        for(int i = 0; i < 999 ; i++) {
	        	int firstPos = buffer[i];
	        	int secondPos = buffer[i+1];
	        	
	        	int XORVal = firstPos ^ secondPos;
	        	System.out.println(XORVal);
	        	output ^= XORVal;
	        	i++;
	        }
	        System.out.println("Final Checksum");
	        System.out.println(output);
	        file.close();
	        fs.close();
	    } 

}
