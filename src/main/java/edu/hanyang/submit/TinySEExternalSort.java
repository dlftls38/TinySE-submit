package edu.hanyang.submit;

import java.io.*;
import edu.hanyang.indexer.ExternalSort;

public class TinySEExternalSort implements ExternalSort {

    public static void main(String[] args)  throws IOException{
        String input_path = "D:/github/informationRetrieval/TinySE-submit/target/test-classes/test.data";

        System.out.println("This is input data");
        DataInputStream dataInputStream = new DataInputStream(
                new BufferedInputStream(
                        new FileInputStream(input_path), 1024)
                );
        for(int i=0;i<15;i++){
            System.out.print(i + ": (");
            for(int j=0;j<3;j++){
                int data = dataInputStream.readInt();
                System.out.print(data);
                if(j!=2){
                    System.out.print(", ");
                }
            }
            System.out.println(")");
        }
        dataInputStream.close();


        String sorted_path = "D:/github/informationRetrieval/TinySE-submit/tmp/sorted.data";

        System.out.println("----------sorted data----------");
        DataInputStream dataInputStream2 = new DataInputStream(
                new BufferedInputStream(
                        new FileInputStream(sorted_path), 1024)
        );
        for(int i=0;i<15;i++){
            System.out.print(i + ": (");
            for(int j=0;j<3;j++){
                int data = dataInputStream2.readInt();
                System.out.print(data);
                if(j!=2){
                    System.out.print(", ");
                }
            }
            System.out.println(")");
        }
        dataInputStream2.close();
    }

    public void sort(String infile, String outfile, String tmpDir, int blocksize, int nblocks) throws IOException {
        File tmpfile = new File(tmpDir);
        System.out.println("\n\n"+tmpfile+"\n\n"+outfile+"\n\n");
        tmpfile.mkdir();

        File raw_data = new File(infile);
        File sorted_data = new File(outfile);

        try {

            FileInputStream fis = new FileInputStream(raw_data); //읽을파일
            FileOutputStream fos = new FileOutputStream(sorted_data); //복사할파일

            int fileByte = 0;
            // fis.read()가 -1 이면 파일을 다 읽은것
            while((fileByte = fis.read()) != -1) {
                fos.write(fileByte);
            }
            //자원사용종료
            fis.close();
            fos.close();

        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}