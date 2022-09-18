package basic;

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

/*
Classe principal (WordCount) do contador de palavras
Dentro dessa classe, teremos:
i) Método principal (main): configurações hadoop, input, output, ...
ii) Classe MapX estende a Classe Mapper do Hadoop
iii) Classe ReduceX estende a Classe Reducer do Hadoop
 */


public class WordCount2 {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "wordcount");

        // Registros das classes
        j.setJarByClass(WordCount2.class); //classe principal
        j.setMapperClass(MapForWordCount.class); //classe mapper
        j.setReducerClass(ReduceForWordCount.class); //classe reducer

        // Definição dos tipos de saída (map e reduce)
        j.setMapOutputKeyClass(Text.class); //chave de saída do map
        j.setMapOutputValueClass(IntWritable.class); //valor de saída do map
        j.setOutputKeyClass(Text.class); //chave de saída do reduce
        j.setMapOutputValueClass(IntWritable.class); //valor saída do reduce

        // Cadastrar os arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input); //arquivo de entrada
        FileOutputFormat.setOutputPath(j, output); //arquivo de saída

        // Execução do job
        j.waitForCompletion(true);

    }

    public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

                //obtendo o conteúdo da linha de entrada (value)
                String linha = value.toString().toLowerCase().replaceAll("[^a-z]", " ");

                //obtendo conteudo da linha em palavras
                String[] palavras = linha.split(" ");

                //gerar (chave, valor) com base no vetor de strings palavras
                for (String p: palavras){
                    //registrar (p,1)
                    if(p.length() > 1){
                        Text chaveSaida = new Text(p); //cast de String > Text
                        IntWritable valorSaida = new IntWritable(1); //cast de int para IntWritable

                        //enviando pares (chave, valor) para o Sort/Shuffle (responsável por ordenar as chaves)
                        con.write(chaveSaida, valorSaida);
                    }
                }
        }
    }

    public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            //dada uma chave, somar todas as suas ocorrências
            int soma = 0;
            for (IntWritable v : values){
                soma += v.get();
            }
            IntWritable valorSaida = new IntWritable(soma); //cast de int > IntWritable

            //salva os resultados HDFS
            con.write(key, valorSaida);
        }
    }

}
