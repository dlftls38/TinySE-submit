package edu.hanyang.submit;

import java.io.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import edu.hanyang.indexer.ExternalSort;
import org.apache.commons.lang3.tuple.MutableTriple;

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
        // 1) initial phase
        // 메모리에 올릴 수 있는 전체 사이즈 = blocksize * nblocks(M) / Integer.SIZE
        int nElement = blocksize * nblocks / Integer.SIZE;
        // MutableTriple을 이용해 각 data를 tuple에 담을 것
        ArrayList<MutableTriple<Integer, Integer, Integer>> dataArr = new ArrayList<>(nElement);
        // Disk로부터 Data를 읽을 cursor
        DataInputStream is = new DataInputStream(
                new BufferedInputStream(
                        new FileInputStream(infile), blocksize)
        );




        File file = new File(tmpDir);
        if (!file.exists()) {
            file.mkdir();
        }

        DataOutputStream os = new DataOutputStream(
                new BufferedOutputStream(
                        new FileOutputStream(outfile), blocksize)
        );
        for (int i=0; i<15; i++){
            for (int j=0; j<3; j++){
                // 테스트로 데이터 넣어보기, tmp Directory와 sorted.data 잘 생성됨
                os.writeInt((i + j + 1)*i*j%30);
            }
        }

        // File cursor 닫기
        is.close();
        os.close();

        // 2) n-way merge
//        _externalMergeSort(tmpdir, outfile, 0);
    }

    private void _externalMergeSort(String tmpDir, String outputFile, int step) throws IOException{
        String prevStep = "";
        File[] fileArr = (new File(tmpDir + File.separator + String.valueOf(prevStep))).listFiles();

        //임시로 설정
        int nblocks=0;
        int blocksize=0;
        int cnt=0;



        if(fileArr.length <= nblocks - 1){
            for(File f: fileArr){
                DataInputStream dataInputStream = new DataInputStream(
                        new BufferedInputStream(
                                new FileInputStream(f.getAbsolutePath()), blocksize)
                );
            }
        }
        else{
            for(File f: fileArr){
                cnt++;
                if(cnt == nblocks -1){
//                    n_way_merge();
                }
            }
            _externalMergeSort(tmpDir, outputFile, step+1);
        }
    }

    public void n_way_merge(List<DataInputStream> files, String outputFile) throws IOException {
        PriorityQueue<DataManager> queue = new PriorityQueue<>
                (files.size(), new Comparator<DataManager>() {
            public int compare(DataManager o1, DataManager o2) {
                return o1.tuple.compareTo(o2.tuple);
            }
        });
        while (queue.size() != 0){
            DataManager dm = queue.poll();
//            MutableTriple<Integer, Integer, Integer> tmp = dm.getTuple();
        }
    }

    class DataManager{
        public boolean isEOF = false;
        private DataInputStream dis = null;
        public MutableTriple<Integer, Integer, Integer> tuple = new MutableTriple<Integer, Integer, Integer>(0, 0, 0);
        public DataManager(DataInputStream dis) throws IOException {

        }

        private boolean readNext() throws IOException {
            if (isEOF) return false;
            tuple.setLeft(dis.readInt()); tuple.setMiddle(dis.readInt()); tuple.setRight(dis.readInt());
            return true;
        }

        public void getTuple(MutableTriple<Integer, Integer, Integer> ret)throws IOException {
            ret.setLeft(tuple.getLeft()); ret.setMiddle(tuple.getMiddle()); ret.setRight(tuple.getRight());
            isEOF = (! readNext());
        }
    }
}