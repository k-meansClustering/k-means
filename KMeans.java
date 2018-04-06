/********************************************************************************************                                                                                                                      
Author : Lalit Singh                                                                                                                                                                                               
Date : July 19, 2016                                                                                                                                                                                                
Work : This code needs to be executed in loop to find K-Means Centroid                                                                                                                        
                                                                                                                                                                                                                   
*******************************************************************************************/

import java.io.IOException;
import java.util.StringTokenizer;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class KMeans {
  	public static class KmeansMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
      		//variable decleration for holding centroid
      		//this decleration need to be changes according to the number of centroid being used
      		//right now 2 centroid is being used, if we wish to use 5 centroid, then change value from 2 to 5
      		int centroidCntr = 2;
      		double[][] centroid = new double[centroidCntr][centroidCntr];

      		@Override
      		public void setup(Context context){
	  		Path[] uris = null;
	  		try{
	      			//setup for reading cache file in this case, centroid file
	      			//this setup is for context, means for hdfs context
	      			uris = DistributedCache.getLocalCacheFiles(context.getConfiguration());
	  		}
	  		catch(IOException ex){
	      			System.out.println(ex);
	  		}
	  		BufferedReader br = null;//opening stream reader to read centroid file
	  		try{
	      			br = new BufferedReader(new FileReader(uris[0].toString()));
	  		}
	  		catch(FileNotFoundException ex){
	      			System.out.println(ex);
	 		}
	  		//reading centroid file
	  		try{
	      			String currLine = null;
	      			int Cntr=0;
	      			while((currLine = br.readLine())!=null){
		  			String[] centStr = currLine.split("\t");
		  			if(Cntr<centroidCntr){
		      				//storing x and y for centriod in the cache for being used in Mapper
		      				centroid[Cntr][0] = Double.parseDouble(centStr[1]);
		      				centroid[Cntr][1]= Double.parseDouble(centStr[2]);
		      				Cntr++;
		  			}
	      			}
	  		}
	  		catch(IOException ex){
	      			System.out.println(ex);
	  		}
      		}

      		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

	  		// Read number of centroids and current centroids from file
	  		// calculate distance of given point to each Centroid
	  		// winnerCentroid = centroid with minimarl distance for this point
	  		double distance=0.0;
	  		double minDistance=999999999.99999999;
	  		int winnercentroid=-1;
	  		int i=0;
	  		//double[][] C= new double[2][2];
	  		//float[][] C2= new float[2][2];

	  		/*C[0][0] = 3.0;
	  		C[0][1] = 7.0;
	  		C[1][0] = 9.0;
	  		C[1][1] = 4.0;*/
	  
	  		String line = value.toString();
	  		String[] currStr = line.split("\t");

	  		double x = Double.parseDouble(currStr[0]);
	  		double y = Double.parseDouble(currStr[1]);

	  		for(i=0;i<centroidCntr;i++){
	      			distance = (x - centroid[i][0]) * (x - centroid[i][0]) + (y - centroid[i][1]) * (y - centroid[i][1]);
	      			if(distance<minDistance){
		  			minDistance = distance;
		  			winnercentroid = i;
	      			}
	  		}
	  		String line1=line+ "\t" + minDistance;
			Text val=new Text(line1);
	  		IntWritable winnerCentroid = new IntWritable(winnercentroid);
	  		context.write(winnerCentroid, val);
      		}
  	}

  
 	public static class KmeansReducer extends Reducer<IntWritable,Text,IntWritable,Text> {

      		public void reduce(IntWritable clusterid, Iterable<Text> points,Context context) throws IOException, 															InterruptedException {

	  		int num = 0;
	  		double centerx=0.0;
	  		double centery=0.0;
			double SSE=0.0;
	  		for (Text point : points) {
	      			num++;
	      			String line = point.toString();
	      			String[] currStr = line.split("\t");
	      
	      			double x = Double.parseDouble(currStr[0]);
	      			double y = Double.parseDouble(currStr[1]);
				double z = Double.parseDouble(currStr[2]);
	      			centerx += x;
	      			centery += y;
				SSE +=z;
	  		}
	  		centerx = centerx/num;
	  		centery = centery/num;
	  
	  		String preres = ""+centerx+"\t"+centery + "\t number of points in this cluster\t" + num+ "\t SSE \t" + SSE;
	  		Text result = new Text(preres);
	  		context.write(clusterid, result);
      		}
  	}

    public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	String[] otherArgs = null;

	try{
	    otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	}
	catch(IOException ex){
	    System.out.println(ex);
	}

	if(otherArgs.length!=3){
	    System.err.println("Usage: KMeans <inputFile> <outputFile> <centroidFile>");
	    System.exit(2);
	}
	
	Job job = null;
	try{
	    job = new Job(conf, "KMeans");
	}
	catch(IOException ex){
	    System.out.println(ex);
	}

	try{
	    DistributedCache.addCacheFile(new URI(otherArgs[2]), job.getConfiguration());
	}
	catch(URISyntaxException ex){
	    System.out.println(ex);
	}
	 
	job.setJarByClass(KMeans.class);
	job.setMapperClass(KmeansMapper.class);
	job.setReducerClass(KmeansReducer.class);


	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(Text.class);

	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(Text.class);

	FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

	job.waitForCompletion(true);
    }
}
