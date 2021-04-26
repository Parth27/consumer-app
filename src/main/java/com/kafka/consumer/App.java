package com.kafka.consumer;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        MessageConsumer consumer = new MessageConsumer();
        consumer.run();
    }
}
