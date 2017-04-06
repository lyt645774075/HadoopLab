package com.tech.freturn;

import java.io.IOException;
import java.util.Arrays;

import com.tech.freturn.task.maxTemperature.MaxTemperatureDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import com.tech.freturn.task.maxTemperature.MaxTemperatureMapper;
import com.tech.freturn.task.maxTemperature.MaxTemperatureReducer;

/**
 * @author 天脉(yangtao.lyt)
 * @version 2017年04月05 23:17
 */
public class MaxTemperatureMapperTest {
    @Test
    public void processesValidRecord() throws IOException, InterruptedException {
        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382"
                              + "99999V0203201N00261220001CN9999999N9-00111+99999999999");
        new MapDriver<LongWritable, Text, Text, IntWritable>()
            .withMapper(new MaxTemperatureMapper()).withInput(new LongWritable(0), value)
            .withOutput(new Text("1950"), new IntWritable(-11)).runTest();
    }

    @Test
    public void ignoresMissingTemperatureRecord() throws IOException, InterruptedException {
        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" + // Year ^^^^
                              "99999V0203201N00261220001CN9999999N9+99991+99999999999"); // Temperature ^^^^^
        new MapDriver<LongWritable, Text, Text, IntWritable>()
            .withMapper(new MaxTemperatureMapper()).withInput(new LongWritable(0), value).runTest();
    }

    @Test
    public void returnsMaximumIntegerInValues() throws IOException, InterruptedException {
        new ReduceDriver<Text, IntWritable, Text, IntWritable>()
            .withReducer(new MaxTemperatureReducer())
            .withInput(new Text("1950"), Arrays.asList(new IntWritable(10), new IntWritable(5)))
            .withOutput(new Text("1950"), new IntWritable(10)).runTest();
    }

    @Test
    public void test() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        conf.setInt("mapreduce.task.io.sort.mb", 1);
        Path input = new Path("input/ncdc/micro");
        Path output = new Path("output");
        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(output, true); // delete old output
        MaxTemperatureDriver driver = new MaxTemperatureDriver();
        driver.setConf(conf);
        int exitCode = driver.run(new String[] { input.toString(), output.toString() });
    }


}
