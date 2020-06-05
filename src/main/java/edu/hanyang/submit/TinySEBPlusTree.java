package edu.hanyang.submit;

import edu.hanyang.indexer.BPlusTree;

import java.io.*;
import java.nio.ByteBuffer;

public class TinySEBPlusTree implements BPlusTree{

	static int rootFilePosition;
	static int fanout;
	static int blocksize;
	static RandomAccessFile treefile;
	static Node rootnode;
	String metapath;
	static int theNumberOfNodes;
	static LRUCache caches;
	static ByteBuffer buff;
	static byte[] b;

	@Override
	public void close() throws IOException {
		caches.map.forEach((key,value)->{
			try {
				value.value.writeData(value.value.treeFileOffset);
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
		rootnode.writeData(rootnode.treeFileOffset);
		treefile.close();
		DataOutputStream dos = new DataOutputStream(
				new BufferedOutputStream(
						new FileOutputStream(
								metapath
						),
						blocksize
				)
		);
		dos.writeInt(rootnode.treeFileOffset);
		dos.writeInt(fanout);
		dos.writeInt(blocksize);
		dos.close();
	}



	@Override
	public void insert(int key, int value) throws IOException {
		rootnode.insertValue(key, value);
	}

	@Override
	public void open(String metapath, String savepath, int blocksize, int nblocks) throws IOException {
		this.metapath = metapath;
		theNumberOfNodes = 0;
		int cacheSize = nblocks*4096/blocksize;
		//blocksize : 1024 | nblocks : 2000 | Integer.SIZE : 32 | divide : 8 | fanout : 127 | cacheSize : 8000 | penalty : 4
		//500만 데이터 : 148 + 156 = 305초
		//1500만 데이터 : 579 + 530 = 1110초
		double penalty = 3.5;
		blocksize/=penalty;
		//if(caches._isEmpty())
//			double how_much_do_you_wanna_use_for_cache = 50; // 50이면 전체 가용 메모리의 50%
//			int maxMemory = (int) (Runtime.getRuntime().maxMemory() / 1024);
//			int cacheSize = (int) (maxMemory * (how_much_do_you_wanna_use_for_cache/100));
//			System.out.println("\nCacheSize "+cacheSize+" = 전체 메모리의 "+how_much_do_you_wanna_use_for_cache+"% 사용\n");
		cacheSize*=penalty;
		//System.out.println("\nCacheSize "+cacheSize+" at penalty "+penalty+"\n");
		double bonus=1;
		cacheSize*=bonus;
		caches = new LRUCache(cacheSize);
		//mvn clean -D test=BPlusTreeTest#bPlusTreeTestWithLargeFile test



		File tmpdir = new File("./tmp");
		if (!tmpdir.exists()) {
			tmpdir.mkdir();
		}
		File treenode = new File(savepath);
		if(!treenode.exists()) {
			treenode.createNewFile();
		}
		this.treefile = new RandomAccessFile(treenode, "rw");
		File metafile = new File(metapath);
		if(metafile.exists() && metafile.length()>0){
			DataInputStream dis = new DataInputStream(
					new BufferedInputStream(
							new FileInputStream(metapath),
							blocksize
					)
			);
			this.rootFilePosition = dis.readInt();
			this.fanout = dis.readInt();
			this.blocksize = dis.readInt();
			dis.close();
		}
		else{
			metafile.createNewFile();
			this.rootFilePosition = 0;
			int divide = Integer.SIZE/4;
			this.fanout = blocksize/(divide)-1;
//				System.out.println("blocksize : " + blocksize + " | nblocks : " + nblocks +  " | Integer.SIZE : " + Integer.SIZE + " | divide : " + divide + " | fanout : " + fanout + " | penalty : " + penalty);
			if(this.fanout<=10){
				this.fanout = 10;
				blocksize = fanout*100;
			}
			this.blocksize = blocksize;

		}
		if(treefile.length() > 0) {
			rootnode = makeNode(rootFilePosition);
		}
		else{
			rootnode = new LeafNode();
		}
		b = new byte[this.blocksize];
		buff = ByteBuffer.wrap(b);
	}

	@Override
	public int search(int key) throws IOException {
		return rootnode.searchValue(key);
	}



	static Node makeNode(int treeFileOffset) throws IOException{
		treefile.seek(treeFileOffset);
		int isLeaf = treefile.readInt();
		if (isLeaf == 1) {
			return new LeafNode(treeFileOffset);
		} else {
			return new InternalNode(treeFileOffset);
		}
	}
}