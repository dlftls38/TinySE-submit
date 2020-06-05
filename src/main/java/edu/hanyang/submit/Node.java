package edu.hanyang.submit;

import java.io.IOException;

public abstract class Node {

    int[] keys;

    int key_size;

    int treeFileOffset;

    abstract void writeData(int offset) throws IOException;

    abstract void insertValue(int key, int value) throws IOException;

    abstract int splitNode() throws IOException;

    abstract boolean nodeIsFull();

    abstract int searchValue(int key) throws IOException;
}