
import java.util.ArrayList;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author eric
 */
public abstract class Importer<T extends From,K extends To> {
    
    private ArrayList<K> outputs = new ArrayList<K>();
    
    public void doImport(T[] inputs) throws InstantiationException, IllegalAccessException {
        for(T input: inputs) {
            K output = getTo().newInstance();
            map(input,output);
            outputs.add(output);
        }
    }
    
    protected abstract void map(T from, K to);
    protected abstract Class<T> getFrom();
    protected abstract Class<K> getTo();
    
}
