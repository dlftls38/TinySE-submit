package edu.hanyang.submit;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import java.net.URLDecoder;
import java.io.File;

import edu.hanyang.indexer.ExternalSort;

public class TinySEExternalSort implements ExternalSort {
	public void sort(String infile, String outfile, String tmpdir, int blocksize, int nblocks) throws IOException {
		infile = URLDecoder.decode(infile, "UTF-8");
		outfile = URLDecoder.decode(outfile, "UTF-8");
		File tmpfile = new File(tmpdir);
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