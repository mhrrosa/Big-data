package TDE1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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


public class Exercicio6 {


    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("./in/operacoes_comerciais_inteira.csv");

        // arquivo de saida
        Path output = new Path("./output/Exercicio6.txt");
        // criacao do job e seu nome
        Job j = new Job(c, "Exercicio6");

        // registro das classes
        j.setJarByClass(Exercicio6.class);
        j.setMapperClass(Exercicio6.MapUnityTypeYear.class);
        j.setReducerClass(Exercicio6.ReduceUnityTypeYear.class);
        //Combiner
        //j.setCombinerClass(Q5.ReduceAvgCommodityUTypeYearFlow.class);

        // definicao dos tipos de saida
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(DoubleWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapUnityTypeYear extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String linha = value.toString();
            String valores[]=linha.split(";");
            if(!valores[1].equals("year")){//Verificação para pular a primeira linha
                con.write(new Text(valores[7]+" "+valores[1]+": "),new DoubleWritable(Double.parseDouble(valores[5])));
            }
        }
    }

    public static class ReduceUnityTypeYear extends Reducer<Text, DoubleWritable, Text, Text> {

        public void reduce(Text key, Iterable<DoubleWritable> values, Context con)
                throws IOException, InterruptedException {
            double MAX = Double.MIN_VALUE;
            double somaTotal = 0.0;
            double qtdValores = 0.0;

            for (DoubleWritable v : values){
                qtdValores+=1.0;
                double valor = v.get();
                somaTotal += valor;
                if(valor>MAX){MAX=valor;}

            }
            //Monta uma STRING para o retorno dos valores desejados
            String valor = Double.toString(MAX);
            con.write(new Text(key.toString()), new Text(valor));

        }
    }

}