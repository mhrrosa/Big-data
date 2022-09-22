package TDE1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class Exercicio3 {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path("./in/operacoes_comerciais_inteira.csv");

        Path output = new Path("./output/MaiorCommodity_resultado.txt");

        // criacao do job e seu nome
        Job j = new Job(c, "maiorCommodity");

        // Registros das classes
        j.setJarByClass(Exercicio3.class);
        j.setMapperClass(Exercicio3.MapForMaiorCommodity.class);
        j.setReducerClass(Exercicio3.ReduceForMaiorCommodity.class);

        // Definição dos tipos de saída (map e reduce)
        j.setMapOutputKeyClass(Text.class); //chave de saída do map
        j.setMapOutputValueClass(TransacaoWritable.class); //valor de saída do map
        j.setOutputKeyClass(Text.class); //chave de saída do reduce
        j.setOutputValueClass(Text.class); //valor saída do reduce

        // Cadastrar os arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input); //arquivo de entrada
        FileOutputFormat.setOutputPath(j, output); //arquivo de saída

        // Execução do job
        j.waitForCompletion(true);

    }

    public static class MapForMaiorCommodity extends Mapper<Object, Text, Text, TransacaoWritable> {
        public void map(Object key, Text value, Context con)
                throws IOException, InterruptedException{

            String linha = value.toString();

            String[] caracteres = linha.split(";");

            Text commodity = new Text("Commodity com maior transação: ");

            //Apenas no ano de 2016
            if(caracteres[1].equals("2016")){
                TransacaoWritable valorSaida = new TransacaoWritable();
                valorSaida.setCommodity(caracteres[3]);
                valorSaida.setN(1);
                con.write(commodity, valorSaida);
            }
        }
    }

    public static class ReduceForMaiorCommodity extends Reducer<Text, TransacaoWritable, Text, Text> {
        public void reduce(Text key, Iterable<TransacaoWritable> values, Context con)
                throws IOException, InterruptedException{

            // Cria um HashMap para armazenar os valores
            HashMap<String, Integer> hashMap = new HashMap<>();

            // Para cada valor
            for (TransacaoWritable v : values
            ) {
                // Se já estiver no hashmap
                if (hashMap.get(v.getCommodity()) != null) {
                    // Incrementa o valor armazenado
                    Integer novoN = hashMap.get(v.getCommodity()) + v.getN();
                    hashMap.put(v.getCommodity(), novoN);
                } else {
                    // Se for novo, apenas é colocado no hashmap
                    hashMap.put(v.getCommodity(), v.getN());
                }
            }

            Integer maiorN = 0;
            // Variável de saída
            String saida = "";

            // Para cada valor na HashMap
            for (HashMap.Entry<String, Integer> pair : hashMap.entrySet()) {
                // Se o valor for maior que o armazanado
                if (pair.getValue() > maiorN) {
                    // Os valores são sobreescritos
                    maiorN = pair.getValue();
                    saida = "\n" + pair.getKey() + " " + pair.getValue();
                } else if (pair.getValue().equals(maiorN)) {
                    // Se forem iguais, os resultados são concatenados
                    saida += "\n" + pair.getKey() + " " + pair.getValue();
                }
            }

            con.write(key, new Text(saida));
        }
    }
}
