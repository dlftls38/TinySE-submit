package edu.hanyang;
import java.io.*;

public class TestadtaViewer {
    public static void main(String[] args) throws IOException {
        DataInputStream dataInputStream = new DataInputStream(new FileInputStream("./test.data"));
        int one = dataInputStream.readInt();
        System.out.println(one);
        int one2 = dataInputStream.readInt();
        System.out.println(one2);
        int one3 = dataInputStream.readInt();
        System.out.println(one3);
        int one4 = dataInputStream.readInt();
        System.out.println(one4);
        int one5 = dataInputStream.readInt();
        System.out.println(one5);
        int one6 = dataInputStream.readInt();
        System.out.println(one6);
        int one7 = dataInputStream.readInt();
        System.out.println(one7);
        int one8 = dataInputStream.readInt();
        System.out.println(one8);
        int one9 = dataInputStream.readInt();
        System.out.println(one9);
        dataInputStream.close();
    }
}