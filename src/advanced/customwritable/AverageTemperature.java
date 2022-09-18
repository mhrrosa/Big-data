package advanced.customwritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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

public class AverageTemperature {

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "media");

        //registro da classe
        j.setJarByClass(AverageTemperature.class);
        j.setMapperClass(MapForAverage.class);
        j.setCombinerClass(CombineForAverage.class);
        j.setReducerClass(ReduceForAverage.class);

        //definição tipos de saida
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(FireAvgTempWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);

        //cadastrar arquivos entradas e saidas
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        //execução do job
        j.waitForCompletion(true);
    }
    public static class MapForAverage extends Mapper<LongWritable, Text, Text, FireAvgTempWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            //convete a variavel value (que representa a linha do arquivo) de text para string
            String linha = value.toString();

            String[] colunas = linha.split(",");

            //armazena somente a temperatura (posição 8) - necessário converter para float
            float temperatura = Float.parseFloat(colunas[8]);

            //armazena somente o mês da ocorrência
            Text mes = new Text(colunas[2]);

            //ocorrencia
            int n = 1;

            con.write(new Text("media_global"), new FireAvgTempWritable(temperatura,n));
            con.write(mes, new FireAvgTempWritable(temperatura, n));

        }
    }

    public static class ReduceForAverage extends Reducer<Text, FireAvgTempWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<FireAvgTempWritable> values, Context con)
                throws IOException, InterruptedException {
            float somatemp = 0;
            int somaNs = 0;
            for(FireAvgTempWritable o :values){
                somatemp +=o.getSomaTemperatura();
                somaNs += o.getOcorrencia();
            }
            FloatWritable media = new FloatWritable(somatemp/somaNs);
            con.write(key,media);
        }
    }

    public static class CombineForAverage extends Reducer<Text, FireAvgTempWritable, Text, FireAvgTempWritable>{
        public void reduce(Text key, Iterable<FireAvgTempWritable> values, Context con) throws IOException, InterruptedException {
            //no combiner, vamos somas as temperaturas parciais do bloco e as oco
            float somatemp = 0;
            int somaNs = 0;
            //somando temperaturas e ocorrências
            for(FireAvgTempWritable o :values){
                somatemp +=o.getSomaTemperatura();
                somaNs += o.getOcorrencia();
            }
            con.write(key, new FireAvgTempWritable(somatemp, somaNs));
        }
    }

}