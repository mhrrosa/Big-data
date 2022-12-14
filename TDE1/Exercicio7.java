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
import java.util.HashMap;

public class Exercicio7 {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path("./in/operacoes_comerciais_inteira.csv");

        Path output = new Path("./output/Exercicio7.txt");

        // Criação do job e seu nome
        Job j = new Job(c, "Exercicio7");

        // Registros das classes
        j.setJarByClass(Exercicio7.class); //classe principal
        j.setMapperClass(MapForFluxoAno.class); //classe mapper
        j.setReducerClass(ReduceFluxoAno.class); //classe reducer

        // Definição dos tipos de saída (map e reduce)
        j.setMapOutputKeyClass(Text.class); //chave de saída do map
        j.setMapOutputValueClass(IntWritable.class); //valor de saída do map
        j.setOutputKeyClass(Text.class); //chave de saída do reduce
        j.setOutputValueClass(IntWritable.class); //valor saída do reduce

        // Cadastrar os arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input); //arquivo de entrada
        FileOutputFormat.setOutputPath(j, output); //arquivo de saída

        // Execução do job
        j.waitForCompletion(true);
    }


    public static class MapForFluxoAno extends Mapper<LongWritable, Text, Text, IntWritable>{
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();

            String[] valores = linha.split(";");

            // A chave é o ano e o fluxo
            Text chave = new Text(valores[1] + " " + valores[4]+" : ");


            if(!valores[1].equals("year")){
            // Passando isso pro reduce
                con.write(chave, new IntWritable(1));
            }
        }
    }

    public static class ReduceFluxoAno extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
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
