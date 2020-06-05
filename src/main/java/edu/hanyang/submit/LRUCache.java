package edu.hanyang.submit;

import java.io.IOException;
import java.util.HashMap;

public class LRUCache {
   class Data {
        int key;
        Node value;
        Data pre;
        Data next;
        public Data(int key, Node value){
            this.key = key;
            this.value = value;
        }
    }
    int capacity;
    HashMap<Integer, Data> map;
    Data head;
    Data end;
    public LRUCache(int capacity) {
        this.capacity = capacity;
        map = new HashMap<Integer, Data>(capacity);
    }
    public void remove(Data n){
        if(n.pre != null){
            n.pre.next = n.next;
        }
        else{
            head = n.next;
        }
        if(n.next != null){
            n.next.pre = n.pre;
        }
        else{
            end = n.pre;
        }
    }
    public void setHead(Data n){
        n.next = head;
        n.pre = null;
        if(head != null) {
            head.pre = n;
        }
        head = n;
        if(end == null){
            end = head;
        }
    }
    public Node get(int key, int isInsert) throws IOException {
        if(map.containsKey(key)){
            Data n = map.get(key);
            remove(n);
            setHead(n);
            return n.value;
        }
        else {
            Node newNode = TinySEBPlusTree.makeNode(key);
            set(key, newNode, isInsert);
            return newNode;
        }
    }
    public void set(int key, Node value, int isInsert) throws IOException {
        if(map.containsKey(key)){
            Data n = map.get(key);
            n.value = value;
            remove(n);
            setHead(n);
        }
        else{
            Data n = new Data(key,value);
            if(map.size() >= capacity){
                if(isInsert==1){
                    end.value.writeData(end.value.treeFileOffset);
                }
                map.remove(end.key);
                remove(end);
            }
            setHead(n);
            map.put(key,n);
        }
    }
}
