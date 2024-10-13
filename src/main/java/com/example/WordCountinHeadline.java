import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import java.io.BufferedReader;
import java.io.InputStreamReader;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.Collections;

public class WordCountinHeadline {

    // 第一个Job的Mapper用于统计单词的出现
    public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {


        private Set<String> stopWords = new HashSet<>();
        private String stopWordFilePath;


        // 加载停词文件
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 从 Configuration 中获取停用词文件路径
            stopWordFilePath = context.getConfiguration().get("stop.words.path");
            Path stopWordFile = new Path(stopWordFilePath);
            
            // 读取停用词文件
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(stopWordFile)));
            
            String line;
            while ((line = br.readLine()) != null) {
                stopWords.add(line.trim().toLowerCase());  // 所有停词一律小写
            }
            br.close();
        }

        // 处理每一行数据
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            if (fields.length >= 2) {
                String headline = fields[1];
                headline = headline.replaceAll("\\d+", "")     
                            .replaceAll("[^a-zA-Z ]", " ")   
                            .toLowerCase()              
                            .trim()                                   
                            .replaceAll("\\s+", " ");
                String[] words = headline.split(" ");
                for (String word : words) {
                    if (!stopWords.contains(word) && word.matches("[a-zA-Z]+")) {
                        context.write(new Text(word), new IntWritable(1));  // 输出<单词, 1>
                    }
                }
            }
        }
    }

    //剔除第一行（第一行是标签）
    public static class SkipFirstLineInputFormat extends TextInputFormat {

        @Override
        public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
            return new SkipFirstLineRecordReader();  // 返回一个符合要求的 RecordReader
        }

        // 静态的 RecordReader 类，继承 LineRecordReader
        public static class SkipFirstLineRecordReader extends LineRecordReader {
            private boolean firstLineSkipped = false;

            @Override
            public boolean nextKeyValue() throws IOException {
                // 跳过第一行
                if (!firstLineSkipped) {
                    firstLineSkipped = true;
                    // 调用一次 nextKeyValue 跳过第一行
                    super.nextKeyValue();  
                    return super.nextKeyValue();  // 返回第二行及以后的内容
                } else {
                    return super.nextKeyValue();  // 正常处理
                }
            }
        }
    }

    // 第一个Job的Reducer用于统计单词的出现次数
    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));  // 输出<单词, 词频>
        }
    }

    // 第二个Job的Mapper用于倒排输出，键为词频，值为单词
    public static class SortMapper extends Mapper<Object, Text, IntWritable, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t");
            if (parts.length == 2) {
                String word = parts[0];
                int count = Integer.parseInt(parts[1]);
                context.write(new IntWritable(count), new Text(word));  // 输出<词频, 单词>
            }
        }
    }

    // 按照出现次数降序排序
    public static class DescendingCountComparator extends WritableComparator{
        protected DescendingCountComparator() {
            super(IntWritable.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            // 按照出现次数降序排序
            IntWritable val1 = (IntWritable) w1;
            IntWritable val2 = (IntWritable) w2;
            return val2.compareTo(val1);  // 降序排序
        }
    }

    // 第二个Job的Reducer用于输出前100个<排名>：<单词>，<次数>
    public static class SortReducer extends Reducer<IntWritable, Text, Text, NullWritable> {
        private IntWritable result = new IntWritable();
        private int rank = 1;
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text stock : values) {
                if (rank <= 100) {
                    result.set(key.get());  // 设置出现次数
                    context.write(new Text(rank + ":" + stock.toString() + "," + result.toString()), NullWritable.get());  // 输出排名、单词、出现次数
                    rank++;  // 排名++
                } else {
                    return; 
                }
            }
        }
    }
    public static void main(String[] args) throws Exception {

        Configuration conf1 = new Configuration();
        String stopWordFilePath = args[2];  // 获取停用词文件路径
        conf1.set("stop.words.path", stopWordFilePath);
        // 运行第一个Job：统计词频
        Job job1 = Job.getInstance(conf1, "WordCount");
        job1.setJarByClass(WordCountinHeadline.class);
        job1.setMapperClass(WordCountMapper.class);
        job1.setReducerClass(WordCountReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setNumReduceTasks(2);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/temp"));


        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // 运行第二个Job：排序
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Sort word count");
        job2.setJarByClass(WordCountinHeadline.class);
        job2.setMapperClass(SortMapper.class);
        job2.setReducerClass(SortReducer.class);
        job2.setSortComparatorClass(DescendingCountComparator.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);

        // 设置分区器为TotalOrderPartitioner
        //job2.setPartitionerClass(TotalOrderPartitioner.class);
        // 设置采样比例
        //onf.setFloat("mapreduce.job.totalorder.partitioner.sample", 0.1f);

        job2.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job2, new Path(args[1] + "/temp"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/final"));



        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }

    }
}