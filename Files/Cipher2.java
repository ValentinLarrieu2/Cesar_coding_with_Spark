import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

import java.util.StringTokenizer;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

public class Cipher2 {
    public static class TokenizerMapper
            extends Mapper<Object, Text, IntWritable, Text>{

        private Set<String> patternsToSkip = new HashSet<String>();
        private Configuration conf;
        private BufferedReader fis;
        private Text word = new Text();
        private IntWritable myKey = new IntWritable();

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            conf = context.getConfiguration();
            if (conf.getBoolean("Cipher2.skip.patterns", false)) {
                URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
                for (URI patternsURI : patternsURIs) {
                    Path patternsPath = new Path(patternsURI.getPath());
                    String patternsFileName = patternsPath.getName().toString();
                    parseSkipFile(patternsFileName);
                }
            }
        }

        private void parseSkipFile(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while ((pattern = fis.readLine()) != null) {
                    patternsToSkip.add(pattern);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '"
                        + StringUtils.stringifyException(ioe));
            }
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            myKey.set(Integer.parseInt(key.toString()));
            word.set(line);
            context.write(myKey, word);

        }
    }

    public static class IntSumReducer
            extends Reducer<IntWritable,Text,NullWritable,Text> {

        public char cipherLetter(char x, int value){
            int SHIFT=value%26;
            char Zvalue='Z';
            char zvalue='z';
            char Avalue='A';
            char avalue='a';
            if (((int)x <= (int)Zvalue) && ((int)x >= (int)Avalue)){  // we test if it is an upercase letter
                if ((((int)x + SHIFT) <= (int)Zvalue) && (((int)x + SHIFT) >= (int)Avalue)){  // the new letter stay in the interval
                    return (char)((int)x + SHIFT);
                }
                else{ // if it is outside of the interval
                    if (((int)x + SHIFT) > (int)Zvalue){  // the new letter is outside the right part of the interval
                        return (char)((int)Avalue -1 + ((int)x + SHIFT - (int)Zvalue));
                    }
                    else{  // the new letter is outside the left part of the interval
                        return (char)((int)Zvalue +1 + ((int)x + SHIFT - (int)Avalue));
                    }
                }
            }
            if (((int)x <= (int)zvalue) && ((int)x >= (int)avalue)){
                if ((((int)x + SHIFT) <= (int)zvalue) && (((int)x + SHIFT) >= (int)avalue)){
                    return (char)((int)x + SHIFT);
                }
                else{  // It is outside of the interval
                    if (((int)x + SHIFT) > (int)zvalue){  // the new letter is outside the right part of the interval
                        return (char)((int)avalue -1 + ((int)x + SHIFT - (int)zvalue));
                    }
                    else{ // the new letter is outside the left part of the interval
                        return (char)((int)zvalue +1 +((int)x + SHIFT - (int)avalue));
                    }
                }
            }
            else{  // it is not a letter
                return x;
            }
        }

        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            String line="";
            char charact='a';
            String newLine="";
            for (Text val : values) {
                line=val.toString();
                line=line.replaceAll("(.{1})(?!$)", "$1¤");//we separate every char with a -
                StringTokenizer itr = new StringTokenizer(line, "¤"); //We cut at everyChar
                while (itr.hasMoreTokens()) {
                    charact = itr.nextToken().toString().charAt(0);
                    charact = cipherLetter(charact, context.getConfiguration().getInt("SHIFT", 0));
                    newLine = newLine + charact;
                }

            }
            context.write(NullWritable.get(), new Text(newLine));

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInt("SHIFT", Integer.parseInt(args[2]));
        conf.set("mapred.textoutputformat.separator", "");
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        Job job = Job.getInstance(conf, "Cipher2");
        job.setJarByClass(Cipher2.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        List<String> otherArgs = new ArrayList<String>();
        for (int i = 0; i < remainingArgs.length; ++i) {
            if ("-skip".equals(remainingArgs[i])) {
                job.addCacheFile(new Path(remainingArgs[++i]).toUri());
                job.getConfiguration().setBoolean("Cipher2.skip.patterns", true);
            } else {
                otherArgs.add(remainingArgs[i]);
            }
        }
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}