package edu.hanyang.submit;

import java.io.IOException;
import java.util.Arrays;

class LeafNode extends Node {
    int[] values;
    LeafNode() {
        keys = new int[TinySEBPlusTree.fanout-1];
        values = new int[TinySEBPlusTree.fanout];
        key_size=0;
        treeFileOffset = TinySEBPlusTree.theNumberOfNodes * TinySEBPlusTree.blocksize;
    }
    LeafNode(int treeFileOffset) throws IOException {
        keys = new int[TinySEBPlusTree.fanout-1];
        values = new int[TinySEBPlusTree.fanout];
        this.treeFileOffset = treeFileOffset;

        TinySEBPlusTree.treefile.seek(treeFileOffset);
        TinySEBPlusTree.treefile.read(TinySEBPlusTree.b);

        TinySEBPlusTree.buff.clear();
        TinySEBPlusTree.buff.getInt();
        key_size = TinySEBPlusTree.buff.getInt();
        for (int i = 0; i < key_size; i++) {
            keys[i] = TinySEBPlusTree.buff.getInt();
            values[i] = TinySEBPlusTree.buff.getInt();
        }
    }

    @Override
    void writeData(int treeFileOffset) throws IOException {
        TinySEBPlusTree.buff.clear();
        TinySEBPlusTree.buff.putInt(1);
        TinySEBPlusTree.buff.putInt(key_size);
        for (int i = 0; i < key_size; i++) {
            TinySEBPlusTree.buff.putInt(keys[i]);
            TinySEBPlusTree.buff.putInt(values[i]);
        }
        TinySEBPlusTree.treefile.seek(treeFileOffset);
        TinySEBPlusTree.treefile.write(TinySEBPlusTree.b);
    }

    @Override
    void insertValue(int key, int value) throws IOException {

        int valueLocation = Arrays.binarySearch(keys,0,key_size, key);
        int valueIndex = valueLocation >= 0 ? valueLocation : -valueLocation - 1;
        for(int i=key_size-1;i>=valueIndex;i--){
            keys[i+1] = keys[i];
            values[i+1] = values[i];
        }
        keys[valueIndex]=key;
        values[valueIndex]= value;
        key_size++;
        TinySEBPlusTree.caches.set(treeFileOffset, this, 1);

        if (TinySEBPlusTree.rootnode.nodeIsFull()) {
            TinySEBPlusTree.theNumberOfNodes += 1;
            InternalNode newRootNode = new InternalNode();
            newRootNode.keys[0] = splitNode();
            newRootNode.key_size++;
            newRootNode.childrenFilePosition[0] = treeFileOffset;
            newRootNode.childrenFilePosition[1] = TinySEBPlusTree.theNumberOfNodes * TinySEBPlusTree.blocksize;
            TinySEBPlusTree.rootnode = newRootNode;
        }
}


    @Override
    int splitNode() throws IOException {
        TinySEBPlusTree.theNumberOfNodes += 1;
        LeafNode newNode = new LeafNode();
        int from = key_size / 2, to = key_size;
        for(int i=0;i<to - from;i++){
            newNode.keys[i] = keys[i + from];
        }
        newNode.key_size = to - from;
        key_size -= to - from;
        for(int i=0;i<to - from;i++){
            newNode.values[i] = values[i + from];
        }
        TinySEBPlusTree.caches.set(newNode.treeFileOffset, newNode, 1);
        return keys[from];
    }

    @Override
    boolean nodeIsFull() {
        return key_size == TinySEBPlusTree.fanout-1;
    }

    @Override
    int searchValue(int key) {
        int valueIndex = Arrays.binarySearch(keys,0,key_size, key);
        return valueIndex >= 0 ? values[valueIndex] : -1;
    }

}
