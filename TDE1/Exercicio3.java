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


public class Exercicio3 {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // arquivo de entrada
        Path input = new Path("./in/operacoes_comerciais_inteira.csv");

        // arquivo de saida
        Path output = new Path("./output/Exercicio3.txt");

        // Criação do job e seu nome
        Job j = new Job(c, "Exercicio3");

        // registro das classes
        j.setJarByClass(Exercicio3.class);
        j.setMapperClass(MapForMaiorCommodity.class);
        j.setReducerClass(ReduceForMaiorCommodity.class);

        // definicao dos tipos de saida
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(DoubleWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // Execução do job
        j.waitForCompletion(true);
    }

    public static class MapForMaiorCommodity extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();

            String[] valores = linha.split(";");
            //Verificação para pular a primeira linha e pegar apenas o ano de 2016
            if(!valores[1].equals("year") && valores[1].equals("2016")){
                con.write(new Text(valores[3]+ " - "+ valores[4]+" - "+ valores[1]+": "),
                        new DoubleWritable(Double.parseDouble(valores[5])));
            }
        }
    }

    public static class ReduceForMaiorCommodity
            extends Reducer<Text, DoubleWritable, Text, Text> {

        public void reduce(Text key, Iterable<DoubleWritable> values, Context con)
                throws IOException, InterruptedException {
            double somaTotal = 0.0;

            for (DoubleWritable v : values) {
                double valor = v.get();
                somaTotal += valor;
            }

            //Monta uma STRING para o retorno dos valores desejados
            String valor = Double.toString(somaTotal);
            con.write(new Text(key.toString()), new Text(valor));

        }
    }
}