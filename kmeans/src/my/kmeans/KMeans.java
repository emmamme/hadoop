package my.kmeans;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.*;

public class KMeans {

	// MAPPER CODE
	public static class KMeansMapper extends Mapper<Object, Text, IntWritable, Text> {
		
	    public static List<Double[]> centroids;

	    @Override
	    protected void setup(Context context) throws IOException, InterruptedException {
	    	Configuration conf = context.getConfiguration();
	    	FileSystem fs = FileSystem.get(conf);
	    	FSDataInputStream dataInputStream = new FSDataInputStream(fs.open(new Path("/user/emmachen/a1_cent/centroid.txt")));//	        FileInputStream fileInputStream = new FileInputStream("/user/emmachen/a1_cent/centroid.txt");
	        BufferedReader reader = new BufferedReader(new InputStreamReader(dataInputStream));
	        
	        
	        List<Double[]> tmp_centroids = new ArrayList<>();
	        String line;
	        try {
	            while ((line = reader.readLine()) != null) {
	                String[] values = line.split("\t");
	                String[] temp = values[0].split(",");
	                Double[] centroid = new Double[2];
	                centroid[0] = Double.parseDouble(temp[0]);
	                centroid[1] = Double.parseDouble(temp[1]);
	                tmp_centroids.add(centroid);
	            }
	        }
	        finally {
	            reader.close();
	        }
	        
	        centroids = tmp_centroids;
	    }

	    @Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] points = value.toString().split(",");
	        double x = Double.parseDouble(points[0]);
	        double y = Double.parseDouble(points[1]);
	        
	        int index = 0;
	        double minDistance = Double.MAX_VALUE;
	        for (int j = 0; j < centroids.size(); j++) {
	            double distance = euclideanDistance(centroids.get(j)[0], centroids.get(j)[1], x, y);
	            if (distance < minDistance) {
	                index = j;
	                minDistance = distance;
	            }
	        }

	        context.write(new IntWritable(index), value);
			
		}
	    
	    public double euclideanDistance(double x1, double y1, double x2, double y2) {
	        return Math.sqrt(Math.pow(x1 - x2, 2) + Math.pow(y1 - y2, 2));
	    }
	}

	// REDUCER CODE
	public static class KMeansReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	        Double mx = 0d;
	        Double my = 0d;
	        int counter = 0;

	        for (Text value: values) {
	            String[] temp = value.toString().split(",");
	            mx += Double.parseDouble(temp[0]);
	            my += Double.parseDouble(temp[1]);
	            counter ++;
	        }

	        mx = mx / counter;
	        my = my / counter;
	        String centroid = mx + "," + my;

	        context.write(new Text(centroid), key);

		}
	}
	
	public static boolean runJob(Configuration configuration, String in_path) throws Exception {
		
        Job job = Job.getInstance(configuration);
        job.setJobName("KMeans");
        job.setJarByClass(KMeans.class);

        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(in_path));
        FileOutputFormat.setOutputPath(job, new Path(configuration.get("OUT_PATH")));

        return job.waitForCompletion(true);
		
	}
	
    public static String readReducerOutput(Configuration configuration) throws IOException {
        FileSystem fs = FileSystem.get(configuration);
        FSDataInputStream dataInputStream = new FSDataInputStream(fs.open(new Path(configuration.get("OUT_PATH") + "/part-r-00000")));
        BufferedReader reader = new BufferedReader(new InputStreamReader(dataInputStream));
        StringBuilder content = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            content.append(line).append("\n");
        }
        System.out.println("Read reducer output ---------------------------------------------------------------");
        System.out.println(content.toString());
        return content.toString();
    }
    
    public static void writeCentroids(Configuration configuration, String formattedCentroids) throws IOException {
    	
    	System.out.println("Writing centroids ---------------------------------------------------------------");

        FileSystem fs = FileSystem.get(configuration);
        FSDataOutputStream fin = fs.create(new Path("/user/emmachen/a1_cent/centroid.txt"));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fin));
        bw.write(formattedCentroids);
        bw.close();
        fs.close();
    }

	// DRIVER CODE
	public static void main(String[] args) throws Exception {
		
		Configuration configuration = new Configuration();
		
		// Initial centroids:
		System.out.println("Initializing centroids ---------------------------------------------------------------");
		List<Double[]> centroids = new ArrayList<>();
		centroids.add(new Double[]{9.293805975483108,8.886169685374657});
		centroids.add(new Double[]{34.62862904699268,5.535490961190803});
		centroids.add(new Double[]{39.51585386054981,-1.3613455916076407});
		centroids.add(new Double[]{33.920720753353066,7.415095526403888});
		
        int counter = 0;
        StringBuilder centroidsBuilder = new StringBuilder();
        for (Double[] centroid : centroids) {
            centroidsBuilder.append(centroid[0].toString());
            centroidsBuilder.append(",");
            centroidsBuilder.append(centroid[1].toString());
            centroidsBuilder.append("\t");
            centroidsBuilder.append("" + counter++);
            centroidsBuilder.append("\n");
        }

        String centroids_string = centroidsBuilder.toString(); 
        System.out.println(centroids_string);
        writeCentroids(configuration, centroids_string);
		
        System.out.println("Start KMeans clustering ---------------------------------------------------------------");
		int iteration = 0;
		while(iteration < 15) {
			configuration.set("OUT_PATH", args[1] + "-" + iteration);
			
            if (!runJob(configuration, args[0])) {

                System.exit(1);
            }
            // reads reducer output file
            String newCentroids = readReducerOutput(configuration);
            writeCentroids(configuration, newCentroids);
            
			iteration ++;
		}
	}
}
