import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.*;

public class CopyOfBigram_Word_Count {
        public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
                private final static IntWritable one = new IntWritable(1);
                private Text word = new Text();
                public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
              
                        String s = value.toString();
                        String lle = s.replaceAll("[^0-9,]","");//remove special
                        String le = java.util.Arrays.toString(lle.split("(?<=\\G..)"));//every two
                        String le2 = le.replaceAll("[^0-9,]","");//remove "[,]"
                        String[] arr = le2.split(",");//split
                        for (String a : arr){
                        
                                word.set(String.valueOf(a));
                                context.write(word, one);
                               
                        }      
                       
                       
                }
        }
        public static class IntSumReducer extends Reducer<Text,IntWritable, Text, IntWritable>{
                Text max_tmpWord = new Text("");
                Text min_tmpWord = new Text("");
                int MaxFrequency = 0;
                int MinFrequency = 100;
                public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
                        //private IntWritable result = new IntWritable();
                        int sum = 0;                   
                        for(IntWritable val : values){
                                sum+=val.get();

                        }
                        if(sum > MaxFrequency){
                                MaxFrequency = sum;
                                max_tmpWord = key;
                                context.write(max_tmpWord, new IntWritable(MaxFrequency));
                        }

                        if(sum <= MinFrequency){
                                MinFrequency = sum;
                                min_tmpWord = key;
                                context.write(min_tmpWord, new IntWritable(MinFrequency));
                        }
               
               
               
                }
        }
       
        public static class Reduce extends Reducer<Text, IntWritable, Text,FloatWritable>{
                private FloatWritable result = new FloatWritable();
                Float average = 0f;
                Float count = 0f;
                long sum1 = 0;
                public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
                       
                    for(IntWritable val: values){
                            sum1+= val.get();
                    }
                       
                    count+=1;
                    average = sum1/count;
                    result.set(average);
                    Text sumText = new Text("average");
                    context.write(key, result);
                }
        }
        public static void main(String[] args) throws Exception{
                Configuration conf = new Configuration();
                Job job = Job.getInstance(conf,"word count");
                job.setJarByClass(CopyOfBigram_Word_Count.class);
                job.setMapperClass(TokenizerMapper.class);
                job.setCombinerClass(IntSumReducer.class);
                job.setReducerClass(IntSumReducer.class);
                //job.setReducerClass(Reduce.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
               
               
                FileInputFormat.addInputPath(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));
                System.exit(job.waitForCompletion(true)? 0 : 1);
               
        }
}
