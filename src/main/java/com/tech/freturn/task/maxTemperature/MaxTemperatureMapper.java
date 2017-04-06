package com.tech.freturn.task.maxTemperature;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author 天脉(yangtao.lyt)
 * @version 2017年04月05 23:21
 */
public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static NcdcRecordParser parser = new NcdcRecordParser();

    public void map(LongWritable key, Text value, Context context) throws IOException,
                                                                   InterruptedException {

        parser.parse(value);
        if (parser.isValidTemperature()) {
            context.write(new Text(parser.getYear()), new IntWritable(parser.getAirTemperature()));
        }
    }
}
