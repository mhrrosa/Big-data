package TDE1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;


public class Exercicio3 {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("./in/operacoes_comerciais_inteira.csv");

        // arquivo de saida
        Path output = new Path("./output/Exercicio3.txt");

        // criacao do job e seu nome
        Job j = new Job(c, "Exercicio3");

        // registro das classes
        j.setJarByClass(Exercicio3.class);
        j.setMapperClass(Exercicio3.MapFTYCount.class);
        j.setReducerClass(Exercicio3.ReduceFTYCount.class);
        //Combiner
        j.setCombinerClass(Exercicio3.CombinerFTYCount.class);

        // definicao dos tipos de saida
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapFTYCount extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String linha = value.toString();

            String campos[]=linha.split(";");
            if(!campos[1].equals("year")){//Verificação para pular a primeira linha
                con.write(new Text(campos[4]+" "+ campos[1]+": "),new IntWritable(1));
            }
        }
    }
    //Combiner para facilitar o processo
    public static class CombinerFTYCount extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable v : values){
                sum += v.get(); // get retorna o valor em int tradicional
            }
            con.write(key, new IntWritable(sum));

        }
    }

    public static class ReduceFTYCount extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable v : values){
                sum += v.get(); // get retorna o valor em int tradicional
            }
            con.write(new Text(key.toString()), new IntWritable(sum));


        }
    }

}