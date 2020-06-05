package edu.hanyang.submit;

import java.io.IOException;
import java.util.Arrays;

public class InternalNode extends Node {

    int[] childrenFilePosition;

    InternalNode() {
        keys = new int[TinySEBPlusTree.fanout-1];
        childrenFilePosition = new int[TinySEBPlusTree.fanout];
        key_size=0;
        treeFileOffset = (TinySEBPlusTree.theNumberOfNodes * TinySEBPlusTree.blocksize);
    }

    InternalNode(int treeFileOffset) throws IOException {
        keys = new int[TinySEBPlusTree.fanout-1];
        childrenFilePosition = new int[TinySEBPlusTree.fanout];
        this.treeFileOffset = treeFileOffset;

        TinySEBPlusTree.treefile.seek(treeFileOffset);
        TinySEBPlusTree.treefile.read(TinySEBPlusTree.b);

        TinySEBPlusTree.buff.clear();
        TinySEBPlusTree.buff.getInt();
        key_size = TinySEBPlusTree.buff.getInt();
        int i;
        for (i = 0; i < key_size; i++) {
            childrenFilePosition[i] = TinySEBPlusTree.buff.getInt();
            keys[i] = TinySEBPlusTree.buff.getInt();
        }
        if (key_size > 0) {
            childrenFilePosition[i] = TinySEBPlusTree.buff.getInt();
        }
    }

    @Override
    void writeData(int treeFileOffset) throws IOException {
        TinySEBPlusTree.buff.clear();
        TinySEBPlusTree.buff.putInt(0);
        TinySEBPlusTree.buff.putInt(key_size);
        int i;
        for (i = 0; i < key_size; i++) {
            TinySEBPlusTree.buff.putInt(childrenFilePosition[i]);
            TinySEBPlusTree.buff.putInt(keys[i]);
        }
        if (key_size > 0) {
            TinySEBPlusTree.buff.putInt(childrenFilePosition[i]);
        }
        TinySEBPlusTree.treefile.seek(treeFileOffset);
        TinySEBPlusTree.treefile.write(TinySEBPlusTree.b);
    }

    @Override
    void insertValue(int key, int value) throws IOException {
        Node childNode = searchChildNode(key, 1);
        childNode.insertValue(key, value);
        if (childNode.nodeIsFull()) {
            int leftdata = childNode.splitNode();
            int pos = (TinySEBPlusTree.theNumberOfNodes) * TinySEBPlusTree.blocksize;
            insertChild(leftdata, pos);
            TinySEBPlusTree.caches.set(treeFileOffset, this, 1);
        }
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

    void insertChild(int key, int childNodeTreeFileOffset) {
        int childLocation = Arrays.binarySearch(keys,0,key_size, key);
        int childrenFilePositionIndex = childLocation >= 0 ? childLocation + 1 : -childLocation - 1;
        for(int i=key_size-1;i>=childrenFilePositionIndex;i--){
            keys[i+1] = keys[i];
            childrenFilePosition[i+2] = childrenFilePosition[i+1];
        }
        keys[childrenFilePositionIndex]=key;
        childrenFilePosition[childrenFilePositionIndex + 1]=childNodeTreeFileOffset;
        key_size++;
    }

    @Override
    int splitNode() throws IOException {
        int from = key_size / 2, to = key_size;
        TinySEBPlusTree.theNumberOfNodes += 1;
        InternalNode newNode = new InternalNode();

        for(int i=0;i<to - (from + 1);i++){
            newNode.keys[i] = keys[i + from + 1];
        }
        newNode.key_size = to - (from + 1);
        key_size -= to - (from + 1);

        for(int i=0;i<to + 1 - (from + 1);i++){
            newNode.childrenFilePosition[i] = childrenFilePosition[i + from + 1];
        }
        TinySEBPlusTree.caches.set(newNode.treeFileOffset, newNode, 1);
        return keys[from];
    }

    @Override
    boolean nodeIsFull() {
        return key_size == TinySEBPlusTree.fanout-1;
    }

    @Override
    int searchValue(int key) throws IOException {
        return searchChildNode(key, 0).searchValue(key);
    }

    Node searchChildNode(int key, int isInsert) throws IOException {
        int childLocation = Arrays.binarySearch(keys,0,key_size, key);
        int childrenFilePositionIndex = childLocation >= 0 ? childLocation + 1 : -childLocation - 1;
        int childFilePosition = childrenFilePosition[childrenFilePositionIndex];
        return TinySEBPlusTree.caches.get(childFilePosition, isInsert);
    }

}
