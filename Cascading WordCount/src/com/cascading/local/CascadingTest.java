package com.cascading.local;

import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.Aggregator;
import cascading.operation.Function;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexGenerator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.local.TextDelimited;
//import cascading.scheme.hadoop.TextLine;
import cascading.scheme.local.TextLine;
import cascading.tap.Tap;
//import cascading.tap.hadoop.Hfs;
import cascading.tap.local.*;
import cascading.tuple.*;

public class CascadingTest {

         
	    public static void main(String[] args) {
           	
        Properties properties = new Properties();

        AppProps.setApplicationJarClass(properties, CascadingTest.class);
        FlowConnector flowConnector = new LocalFlowConnector();
                   
        // create the source tap
        Scheme sourceScheme = new TextLine( new Fields( "line" ) );
        Tap inTap = new FileTap( sourceScheme, "C:\\Barclays Project\\Data\\Job requirements.txt" );

        //create a head pipe
        Pipe assembly = new Pipe("wordcount");
        
        //Regex to parse out word from the line
        String regex = "(?<!\\pL)(?=\\pL)[^ ]*(?<=\\pL)(?!\\pL)";
        Function function = new RegexGenerator(new Fields("word"),regex);
        assembly = new Each(assembly, new Fields("line"), function);
        
        //Groupby to count the words 
        assembly = new GroupBy(assembly, new Fields("word"));
        
        //Count the words that are GroupBy by the above Pipe
        Aggregator count = new Count( new Fields( "count" ) );
        assembly = new Every( assembly, count );
        
        Scheme sinkScheme = new TextDelimited( new Fields( "word", "count" ) );

        // create the sink tap
        Tap outTap = new FileTap( sinkScheme, "C:\\Barclays Project\\Data\\WordCount.txt" );

        // specify a pipe to connect the taps
        //Pipe copyPipe = new Pipe( "copy" );

        // connect the taps, pipes, etc., into a flow
        // run the flow
        Flow flow = flowConnector.connect("word-connect" , inTap, outTap, assembly );
        flow.complete();
    }
}