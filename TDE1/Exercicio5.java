package TDE1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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


public class Exercicio5 {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("./in/operacoes_comerciais_inteira.csv");

        // arquivo de saida
        Path output = new Path("./output/Exercicio5.txt");

        // criacao do job e seu nome
        Job j = new Job(c, "Exercicio5");

        // registro das classes
        j.setJarByClass(Exercicio5.class);
        j.setMapperClass(Exercicio5.MapMediaCommodityTipoAnoFluxo.class);
        j.setReducerClass(Exercicio5.ReduceMediaCommodityTipoAnoFluxo.class);
        //Combiner
        j.setCombinerClass(Exercicio5.ReduceMediaCommodityTipoAnoFluxo.class);

        // definicao dos tipos de saida
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(DoubleWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapMediaCommodityTipoAnoFluxo extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String linha = value.toString();

            String campos[]=linha.split(";");

            if(!campos[1].equals("year") && campos[0].equals("Brazil") && campos[4].equals("Export")){
                con.write(new Text(campos[7] +" "+ campos[1]+": "),new DoubleWritable(Double.parseDouble(campos[5])));
            }
        }
    }

    public static class ReduceMediaCommodityTipoAnoFluxo extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {


        public void reduce(Text key, Iterable<DoubleWritable> values, Context con)
                throws IOException, InterruptedException {
            double qtdValores = 0.0; //Armazena quantos valores estão associados a uma única chave
            double somaTotal = 0.0; //Armazena a soma de todos os valores associados a uma única chave

            for (DoubleWritable v : values){
                qtdValores+=1.0;
                somaTotal += v.get();
            }
            con.write(new Text(key.toString()), new DoubleWritable(somaTotal/qtdValores));

        }
    }

}