package advanced.entropy;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/*
Classe custom writable com atributos privados, construtor vazio, getters e setters para todos os atributo
A classe também deve ser comparável com ela mesma
 */

public class BaseQtdWritable implements WritableComparable<BaseQtdWritable>{
    private String chave;
    private long qtde;

    public BaseQtdWritable() {
    }

    public BaseQtdWritable(String chave, long qtde) {
        this.chave = chave;
        this.qtde = qtde;
    }

    public String getChave() {
        return chave;
    }

    public void setChave(String chave) {
        this.chave = chave;
    }

    public long getQtde() {
        return qtde;
    }

    public void setQtde(long qtde) {
        this.qtde = qtde;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    public int compareTo(BaseQtdWritable o) {
        if (this.hashCode() < o.hashCode()) {
            return -1;
        } else if (this.hashCode() > o.hashCode()) {
            return +1;
        }
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(chave);
        dataOutput.writeLong(qtde);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        chave = dataInput.readUTF();
        qtde = dataInput.readLong();
    }
}