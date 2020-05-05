
package edu.hanyang.submit;

import java.io.*;
import java.util.*;

import edu.hanyang.indexer.ExternalSort;
import edu.hanyang.utils.DiskIO;
import org.apache.commons.lang3.tuple.MutableTriple;

public class TinySEExternalSort implements ExternalSort {

    public static void main(String[] args)  throws IOException{
        String input_path = "C:/Users/charl/TinySE-submit/target/test-classes/test.data";

        System.out.println("----------test.data----------");
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


        String sorted_path = "C:/Users/charl/TinySE-submit/tmp/sorted.data";

        System.out.println("----------sorted.data----------");
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


        String run_path = "C:/Users/charl/TinySE-submit/tmp/run/0.data";

        System.out.println("----------/run/0.data----------");
        DataInputStream dataInputStream3 = new DataInputStream(
                new BufferedInputStream(
                        new FileInputStream(run_path), 1024)
        );
        for(int i=0;i<15;i++){
            System.out.print(i + ": (");
            for(int j=0;j<3;j++){
                int data = dataInputStream3.readInt();
                System.out.print(data);
                if(j!=2){
                    System.out.print(", ");
                }
            }
            System.out.println(")");
        }
        dataInputStream3.close();
    }

    public void sort(String infile, String outfile, String tmpDir, int blocksize, int nblocks) throws IOException {

        // 1) initial phase

        // �޸𸮿� �ø� �� �ִ� ��ü ������ = blocksize * nblocks (= M) / Integer.SIZE
    	
    	//=============================================
    	// �̰� (Integer.SIZE *3) ������ �ϴ°� �ƴ�??
    	//=============================================
    	
    	int nElement = blocksize * nblocks / Integer.SIZE;

        //tmp ���� ����
        File fileTmpDir = new File(tmpDir);
        if(!fileTmpDir.exists()){
            fileTmpDir.mkdir();
        }

        //test�� sorted.data ����
        File sorted = new File(outfile);
        sorted.createNewFile();

        //test�� sorted.data ä���
        ArrayList<MutableTriple<Integer, Integer, Integer>> tmpdataArr = new ArrayList<>(45);
        DataManager tmpdm = new DataManager();
        tmpdm.openDis(infile, blocksize);
        tmpdm.openDos(outfile, blocksize);
        for(int i=0; i<15; i++){
            for(int j=0; j<3; j++){
                // �׽�Ʈ�� ������ �־��, tmp Directory�� sorted.data �� ������
                MutableTriple<Integer, Integer, Integer> tmptuple = new MutableTriple<>();
                tmpdm.readNext();
                tmpdm.getTuple(tmptuple);
                tmpdataArr.add(tmptuple);
                tmpdm.writeNext(tmpdataArr.get(i*3+j));
            }
        }
        tmpdm.closeDis();
        tmpdm.closeDos();
        

        //initial run�� ����
        int number_of_initial_run=0;
        //File I/O�� ���� �͵��� ó���� DataManager ����
        DataManager dm = new DataManager();
        dm.openDis(infile, blocksize);
        
        //�����ؾ��ϴ� input data�� ���� �� ���� ������ ����
        while(dm.dis.available()!=0){
            ArrayList<MutableTriple<Integer, Integer, Integer>> dataArr = new ArrayList<>(nElement);

            //��� �� �� �ִ� �޸� ũ�⿡ ����, �޸𸮿� �ø� �� �ִ� element�� �� = nElement ��ŭ �����͸� �о� ����
            for(int i=0; i<nElement; i++){
                try{
                    MutableTriple<Integer, Integer, Integer> tuple = new MutableTriple<>();
                    dm.readNext();
                    dm.getTuple(tuple);
                    dataArr.add(tuple);
                }catch (EOFException e) {
                    break;
                }
            }
            dataArr.sort(null);

            String runDir = tmpDir+File.separator+"run";
            File fileRunDir = new File(runDir);
            if(!fileRunDir.exists()){
                fileRunDir.mkdir();
            }

            String initial_run = runDir+File.separator+ number_of_initial_run +".data";
            File fileInitialRun = new File(initial_run);
            //initial run �����ϱ�
            fileInitialRun.createNewFile();
            dm.openDos(initial_run, blocksize);
            for (MutableTriple<Integer, Integer, Integer> curData : dataArr) {
                try{
                    dm.writeNext(curData);
                }catch (EOFException e) {
                    break;
                }
            }
            number_of_initial_run++;
            dm.closeDos();
        }
        // File cursor �ݱ�
        dm.closeDis();

        // initial run �����ؼ� quick sort ���ı��� �Ϸ�! 2) n-way merge�� �����ϸ� ��!

        // 2) n-way merge
        _externalMergeSort(tmpDir, outfile, nblocks, blocksize, 0);
    }

    private void _externalMergeSort(String tmpDir, String outputFile, int nblocks, int blocksize, int step) throws IOException{
        String prevStep = "run" + (step);
        File[] fileArr = (new File(tmpDir + File.separator + String.valueOf(prevStep))).listFiles();
        ArrayList<File> fileList = new ArrayList<>();

        if(fileArr.length <= nblocks - 1){
        	for(File f : fileArr) {
            	fileList.add(f);
            }
        	n_way_merge(fileList, outputFile, blocksize);
        	for (File f : fileArr) {
                f.delete();
            }
        }
        else{
        	for (File f : fileArr) { // (nblocks-1)�� �ɶ����� n_way merge
        		fileList.add(f);
        		if(fileList.size() == nblocks-1) {
        			n_way_merge(fileList, outputFile, blocksize);
        			fileList.clear();
        		}
        	}
        	
        	if(fileList.size()>0) {// ���� (nblocks-1)������ ���� ������ n_way_merge
        		n_way_merge(fileList, outputFile, blocksize);
    			fileList.clear();
        	}
        	
        	for (File f : fileArr) {
                f.delete();
            }
            _externalMergeSort(tmpDir, outputFile, nblocks, blocksize, step+1);
        }
    }

    public void n_way_merge(ArrayList<File> files, String outputFile, int blocksize) throws IOException {
    	
        PriorityQueue<DataManager> queue = new PriorityQueue<>(files.size(), new Comparator<DataManager>() {
                    public int compare(DataManager o1, DataManager o2) { 
                        return o1.tuple.compareTo(o2.tuple);
                    }
                });
        
        for (File f: files) {
        	DataManager dm = new DataManager();
        	dm.openDis(f.getAbsolutePath(), blocksize);
        	queue.add(dm);
        }
        
        DataManager out = new DataManager();
        out.openDos(outputFile, blocksize);
        
        while (queue.size() != 0){
        	DataManager dm = queue.poll();
        	MutableTriple<Integer, Integer, Integer> tmp = new MutableTriple<>();
        	dm.getTuple(tmp);
        	out.writeNext(tmp);
        }
        out.closeDos();
    }

    class DataManager{
        public boolean isEOF = false;
        private DataInputStream dis = null;
        private DataOutputStream dos = null;
        public MutableTriple<Integer, Integer, Integer> tuple = new MutableTriple<Integer, Integer, Integer>(0, 0, 0);
        public DataManager() throws IOException {
        }

        private void openDis(String path, int blocksize) throws FileNotFoundException {
            this.dis = new DataInputStream(
                    new BufferedInputStream(
                            new FileInputStream(path), blocksize)
            );
        }

        private void openDos(String path, int blocksize) throws FileNotFoundException {
            this.dos = new DataOutputStream(
                    new BufferedOutputStream(
                            new FileOutputStream(path), blocksize)
            );
        }

        private void closeDis() throws IOException {
            this.dis.close();
        }

        private void closeDos() throws IOException {
            this.dos.close();
        }

        private void readNext() throws IOException {
            tuple.setLeft(dis.readInt()); tuple.setMiddle(dis.readInt()); tuple.setRight(dis.readInt());
        }

        private void getTuple(MutableTriple<Integer, Integer, Integer> ret) throws IOException {
            ret.setLeft(tuple.getLeft()); ret.setMiddle(tuple.getMiddle()); ret.setRight(tuple.getRight());
        }

        private void writeNext(MutableTriple<Integer, Integer, Integer> ret) throws IOException {
            dos.writeInt(ret.getLeft()); dos.writeInt(ret.getMiddle()); dos.writeInt(ret.getRight());
        }
    }
}