package com.kafka.serializer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

public class ArrayListDeserializer implements Deserializer<ArrayList<String>> {
    @Override
    public void configure(Map<String, ?> config, boolean isKey) {
        // To do
    }

    @Override
    public ArrayList<String> deserialize(String topic, byte[] bytes) {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        DataInputStream in = new DataInputStream(bais);
        ArrayList<String> list = new ArrayList<>();

        try{
            while (in.available() > 0) {
                String element = in.readUTF();
                list.add(element);
            }
        } catch (IOException e){
            e.printStackTrace();
        }

        return list;
    }

    @Override
    public void close() {
        // To do
    }
}
