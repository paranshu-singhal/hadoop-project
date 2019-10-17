package edu.ucr.cs.cs226.psing069;

import java.io.BufferedInputStream;
import  java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HDFSUpload {

    public static void main(String[] args) throws Exception {

        int arg = Integer.parseInt(args[0]);
 
        String localSrc = args[1];
        String destination = args[2];

        if(arg == 0){
            writeToHDFS(localSrc, destination);
        } else if(arg == 1){
            readSequential(localSrc, destination);
        } else if(arg == 2){
            readRandom(localSrc, destination);
        }
    }

    //Task 1: The total time for copying the 2GB file.
    public static void writeToHDFS(String input, String output){

        long startTime = System.nanoTime();  
        InputStream in = null;
        Configuration conf = new Configuration();

        try{
            //Input stream for the file in local file system to be written to HDFS
            in = new BufferedInputStream(new FileInputStream(input));
        
            System.out.println("Connecting to -- "+conf.get("fs.defaultFS"));
            
            //Destination file in HDFS
            FileSystem fs = FileSystem.get(URI.create(output), conf);
            FSDataOutputStream out = fs.create(new Path(output), false);
            
            //Copy file from local to HDFS
            IOUtils.copyBytes(in, out, conf, true);
            System.out.println(input + " copied to HDFS in " + Long.toString((System.nanoTime() - startTime)/1000000) + " ms.");
        } catch(FileNotFoundException e){
            System.out.println("Error: Input file not found");
        } catch(FileAlreadyExistsException e){
            System.out.println("Error: File already exists.");
        } catch(IOException e){
            System.out.println("Error: Unable to write file");
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
        }
        
    }

    //Task 2: The total time to read the 2GB file.
    public static void readSequential(String input, String output){

        long startTime = System.nanoTime();  
        Configuration conf = new Configuration();
        input = conf.get("fs.defaultFS") + input;

        FSDataInputStream in = null;
        try {

            FileSystem fs = FileSystem.get(URI.create(input), conf);

            Path path = new Path(input);
            in = fs.open(path);
            in.seek(0);
            
            OutputStream out = new BufferedOutputStream(new FileOutputStream(output));
            
            //IOUtils.copyBytes(in, out, conf, true);
            byte[] buf = new byte[1024];
            int n;
            long bytesToCopy = fs.getFileStatus(path).getLen();
            while ((n = in.read(buf)) != -1 && bytesToCopy > 0) {
                out.write(buf, 0, (int) Math.min(n, bytesToCopy));
                bytesToCopy -= n;
            }

            out.close();
            System.out.println(input + " copied from HDFS in " + Long.toString((System.nanoTime() - startTime)/1000000) + " ms.");

        } catch(FileNotFoundException e){
            System.out.println("Error: Input file not found");
        } catch(IOException e){
            System.out.println("Error: Unable to write file");
            e.printStackTrace();
        } finally {

            IOUtils.closeStream(in);
        }
        
    }

    //Task 3: The total time to make 2,000 random accesses, each of size 1KB
    public static void readRandom(String input, String output){
        
        long startTime = System.nanoTime();  
        Configuration conf = new Configuration();
        FSDataInputStream in = null;

        try {
            FileSystem fs = FileSystem.get(URI.create(input), conf);
            Path path = new Path(input);
            in = fs.open(path);

            long leftLimit = 0L;
            long rightLimit = (fs.getFileStatus(path).getLen()) - 1024;
            
            OutputStream out = new BufferedOutputStream(new FileOutputStream(output));
            byte[] buf = new byte[1024];

            for(int i=0;i<2000;i++){
                long desired = leftLimit + (long) (Math.random() * (rightLimit - leftLimit));
                in.seek(desired);
                int n = in.read(new byte[1024]);
                out.write(buf, 0, n);
            }
            System.out.println(input + " 2000 random 1KB copied from HDFS in " + Long.toString((System.nanoTime() - startTime)/1000000) + " ms.");
            out.close();
        } catch(FileNotFoundException e){
            System.out.println("Error: Input file not found");
        } catch(IOException e){
            System.out.println("Error: Unable to write file");
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
        }

    }
}