

import org.apache.hadoop.conf.*; 
import org.apache.hadoop.fs.*; 
 
public class HDFSWrite  { 
	
    public static void main ( String [] args ) throws Exception {       
        // The system configuration 
   // creates a configuration object to configure the Hadoop system 
        Configuration conf = new Configuration();  
 
        // Get an instance of the Filesystem using configuration 
        FileSystem fs = FileSystem.get(conf); 
         
   // The path to the file that needs to be written 
        String path_name = "/lab1/newfile"; 
         
        Path path = new Path(path_name); 
         
        // The Output Data Stream to write into 
        FSDataOutputStream file = fs.create(path); 
         
        // Write some data 
        file.writeChars("The first Hadoop Program!"); 
         
        // Close the file and the file system instance 
        file.close();  
        fs.close(); 
    } 
}