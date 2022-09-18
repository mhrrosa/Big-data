package TDE1;

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

public class Exercicio4 {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path("./in/operacoes_comerciais_inteira.csv");

        Path output = new Path("./output/PreocoMedioCommodityAno_resultado.txt");

        // criacao do job e seu nome
        Job j = new Job(c, "PreocoMedioCommodityAno");

        // Registros das classes
        j.setJarByClass(Exercicio4.class);
        j.setMapperClass(Exercicio4.MapForPrecoMedioAno.class);
        j.setReducerClass(Exercicio4.ReduceForPrecoMedioAno.class);

        // Definição dos tipos de saída (map e reduce)
        j.setMapOutputKeyClass(Text.class); //chave de saída do map
        j.setMapOutputValueClass(TransacaoWritable.class); //valor de saída do map
        j.setOutputKeyClass(Text.class); //chave de saída do reduce
        j.setOutputValueClass(FloatWritable.class); //valor saída do reduce

        // Cadastrar os arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input); //arquivo de entrada
        FileOutputFormat.setOutputPath(j, output); //arquivo de saída

        // Execução do job
        j.waitForCompletion(true);

    }
    public static class MapForPrecoMedioAno extends Mapper<Object, Text, Text, TransacaoWritable> {
        public void map(Object key, Text value, Context con)
                throws IOException, InterruptedException{
            // Cada linha
            String conteudo = value.toString();
            // Separar por colunas
            String[] valores = conteudo.split(";");

            // Ignora o header e vazio
            if (valores[6].equals("weight_kg") || valores[6].equals("")) return;

   
            Text chave = new Text(valores[3] +" "+ valores[1]);

            // O valor é o peso + n
            TransacaoWritable valor = new TransacaoWritable();
            valor.setN(1);
            valor.setPeso(Long.parseLong(valores[6]));

            // Passando isso pro reduce
            con.write(chave, valor);

        }
    }

        public static class ReduceForPrecoMedioAno extends Reducer<Text, TransacaoWritable, Text, FloatWritable> {
            public void reduce(Text key,
                               Iterable<TransacaoWritable> values,
                               Context context) throws IOException, InterruptedException {

                float soma = 0;
                int nTotal = 0;

                // Para cada valor
                for (TransacaoWritable v : values
                ) {
                    soma += v.getPeso();
                    nTotal += v.getN();
                }


                // Variável de saída
                float media = soma/(float)nTotal;
                FloatWritable saida = new FloatWritable(media);

                context.write(key, saida);

            }
        }

}
