package TDE1;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class TransacaoWritable implements Writable {
    private int n = 0;
    private int codigo = 0;
    private int quantidade = 0;
    private long valor = 0;
    private long peso = 0;
    private String pais = "";
    private String commodity = "";
    private String fluxo = "";
    private String unidade = "";
    private String categoria = "";

    TransacaoWritable() {
    }

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }

    public int getCodigo() {
        return codigo;
    }

    public void setCodigo(int codigo) {
        this.codigo = codigo;
    }

    public int getQuantidade() {
        return quantidade;
    }

    public void setQuantidade(int quantidade) {
        this.quantidade = quantidade;
    }

    public long getValor() {
        return valor;
    }

    public void setValor(long valor) {
        this.valor = valor;
    }

    public long getPeso() {
        return peso;
    }

    public void setPeso(long peso) {
        this.peso = peso;
    }

    public String getPais() {
        return pais;
    }

    public void setPais(String pais) {
        this.pais = pais;
    }

    public String getCommodity() {
        return commodity;
    }

    public void setCommodity(String commodity) {
        this.commodity = commodity;
    }

    public String getFluxo() {
        return fluxo;
    }

    public void setFluxo(String fluxo) {
        this.fluxo = fluxo;
    }

    public String getUnidade() {
        return unidade;
    }

    public void setUnidade(String unidade) {
        this.unidade = unidade;
    }

    public String getCategoria() {
        return categoria;
    }

    public void setCategoria(String categoria) {
        this.categoria = categoria;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        n = Integer.parseInt(dataInput.readUTF());
        peso = Long.parseLong(dataInput.readUTF());
        valor = Long.parseLong(dataInput.readUTF());
        commodity = dataInput.readUTF();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(String.valueOf(n));
        dataOutput.writeUTF(String.valueOf(peso));
        dataOutput.writeUTF(String.valueOf(valor));
        dataOutput.writeUTF(String.valueOf(commodity));
    }
}
