#include <iostream>
#include <fstream>
#include <cstring>
#include <random>
#include <math.h>
#include <iomanip>
#include <string>
#include <map>
#include <cmath>
#include <sstream>
#include <chrono> 
#include <queue>
#include <bits/stdc++.h>
#include "QmaxKV.hpp"

using namespace std;
using namespace std::chrono; 

map<long long, int> flows;

bool weighted=true;
int URWA = 1;
int traceSize = 50000000;
int size=10000;
int m=0;
float gamm=0.25;

pair<long long,int>* Packets;
int c=0;
string Path="/home/tayh/Downloads/HH/trace/chicago/Ttrace.txt";

// priority_queue<pair<long long,int>> Packets;

priority_queue<pair<long long,double>> q;

QMaxKV *qmax= new QMaxKV(size, gamm);
// int updates=0

void readFileFast(ifstream &file, int(*lineHandler)(char*str, int length, int absPos)){
        int BUF_SIZE = 10000000*128;
        file.seekg(0,ios::end);
        ifstream::pos_type p = file.tellg();
#ifdef WIN32
        long fileSize = *(int*)(((char*)&p) +8);
#else
        long fileSize = p;
#endif
//         fileSize = 50000000*1024*52;
        file.seekg(0,ios::beg);
//         BUF_SIZE = min(BUF_SIZE, fileSize);
        char* buf = new char[BUF_SIZE];
        int bufLength = BUF_SIZE;
        file.read(buf, bufLength);

        int strEnd = -1;
        int strStart;
        int bufPosInFile = 0;
        
        int ii=100000000;
        while (bufLength > 0 && ii>0) {
            int i = strEnd + 1;
            strStart = strEnd;
            strEnd = -1;
            for (; i < bufLength && i + bufPosInFile < fileSize; i++) {
                if (buf[i] == '\n') {
                    strEnd = i;
                    break;
                }
            }

            if (strEnd == -1) { // scroll buffer
                if (strStart == -1) {
                    ii--;
                    try{
                        if(lineHandler(buf + strStart + 1, bufLength, bufPosInFile + strStart + 1)==1){
                                return;
                        }
                    }
                    catch(exception& e){
                        cout<<"heeeeeeeeeeeeeeeeeeeee"<<endl;
                        cout<< string(buf+ strStart + 1, strEnd - strStart)<<endl;
                    }
                    bufPosInFile += bufLength;
                    if(fileSize - bufPosInFile < bufLength)
                        bufLength = int(fileSize - bufPosInFile);
                    delete[]buf;
                    buf = new char[bufLength];
                    file.read(buf, bufLength);
                } else {
                    int movedLength = bufLength - strStart - 1;
                    memmove(buf,buf+strStart+1,movedLength);
                    bufPosInFile += strStart + 1;
                    int readSize = bufLength - movedLength;
                    if(readSize > fileSize - bufPosInFile - movedLength)
                        readSize = fileSize - bufPosInFile - movedLength;

                    if (readSize != 0)
                        file.read(buf + movedLength, readSize);
                    if (movedLength + readSize < bufLength) {
                        char *tmpbuf = new char[movedLength + readSize];
                        memmove(tmpbuf,buf,movedLength+readSize);
                        delete[]buf;
                        buf = tmpbuf;
                        bufLength = movedLength + readSize;
                    }
                    strEnd = -1;
                }
            } else {
                ii--;
                try{
                    if(lineHandler(buf+ strStart + 1, strEnd - strStart, bufPosInFile + strStart + 1)==1){
                        return;
                    }   
                }
                catch(int e){
                    cout<< string(buf+ strStart + 1, strEnd - strStart)<<endl;
                }
            }
            
            
        }
}











void insertU(long long flowId, int packetId, int w){
    m++;
//     int tempPacketId = flowId;
//     int newid = packetId;
//     while(tempPacketId>1){
//         int n = tempPacketId%10;
//         tempPacketId/=10;
//         newid*=10;
//         newid+=n;
//     }
    srand(flowId*packetId);
    double random = double(rand())/double(RAND_MAX);
    double p = pow(random,1.0/(double)w);
    int c=0;
    while(c<w  && (p > qmax->getMinimalVal())){
        qmax->insert(key(flowId),val(p));
//         updates+=1;
        random = double(rand())/double(RAND_MAX);
        p*=pow(random,1.0/(double)(w-c));
        c++;
    }
}

        
        
        
        
void insertN(long long flowId, int packetId, int w){
    m++;
//     int tempPacketId = flowId;
//     int newid = packetId;
//     while(tempPacketId>1){
//         int n = tempPacketId%10;
//         tempPacketId/=10;
//         newid*=10;
//         newid+=n;
//     }
    srand(flowId*packetId);
    double random = double(rand())/double(RAND_MAX);
    double p = (double)w/random; 
    if(p > qmax->getMinimalVal()){
        qmax->insert(key(flowId),val(p));
//         updates+=1;
    }
}




    
    

    
    


int lineHandler(char*buf, int l, int pos){
    if(buf==0) return 0;
    string s = string(buf, l);
//     cout<<s<<endl;
    if(s.at(2)==' '){
        int i=3;
        char ch = s.at(i);
        i++;
        string src="";
        while(ch!=' ') {
            if(ch!='.') src.push_back(ch);
            ch = s.at(i);
            i++;
        } 
        
        
        
        i+=2;
        string dist="";
        ch = s.at(i);
        i++;
        while(ch!=' ') {
            if(ch!='.') dist.push_back(ch);
            ch = s.at(i);
            i++;
        } 
        
        
        ch = s.at(i);
        i++;
        string weight="";
        
        if(ch == 'U'){
            i+=11;
            ch = s.at(i);
            if(ch == 'g'){
                return 0;
            }
            i++;
            while(ch!='\n') {
                weight.push_back(ch);
                ch = s.at(i);
                i++;
            } 
        }
        else if(ch == 't'){
            i+=3;
            ch = s.at(i);
            i++;
            while(ch!='\n') {
                weight.push_back(ch);
                ch = s.at(i);
                i++;
            } 
        }
        else{
            return 0;
        }
        
        long long flowId=abs(stoll(dist, nullptr, 10)*stoll(src, nullptr, 10));
        int w=1;
        try{
            w = stoi(weight);
        }
        catch(exception& e){
            w=1;
        }
        
        if(w<=0 | !weighted){
            w=1;
        }
        
        
        Packets[c] = make_pair(flowId,w);
        c++;
        if(c>traceSize){
            
            
            //####################### start timer  ##############################
            auto start = high_resolution_clock::now();
            
            while(c!=0){
                pair<long long,int> t = Packets[c];
                c--;
                if(URWA==1){
                    insertU(t.first, m, t.second);
                }
                else if(URWA==0){
                    insertN(t.first, m, t.second);
                }
            }
            
            
            auto end = high_resolution_clock::now(); 
            //####################### stop timer  ################################
            
            
            auto duration = duration_cast<microseconds>(end - start);
            ofstream outputFile;
            outputFile.open("res.txt", ios::app);
            outputFile << Path <<" "<< URWA <<" "<< gamm << " "<< size<<endl;  
            outputFile << (double(duration.count())/double(1))/1000000.0 << endl;
            outputFile.close();
            
            
            return 1;
        }
    }
    return 0;

}













void loadFile(){
    free(qmax);
    qmax = new QMaxKV(size, gamm);
    flows.clear();
    ifstream infile(Path);
    try{
    readFileFast(infile,lineHandler);
    }
     catch(int e){
                    cout<< "here"<<endl;
                }
}

int main(int argc, char **argv) {
    
    URWA = stoi(argv[4]);
    
    
    Path=argv[1];    
    size = stoi(argv[2]);
    gamm = stod(argv[3]);
    traceSize = stoi(argv[5]);


    cout<< Path <<" "<< URWA <<" "<< gamm << " "<< size<<endl; 
    int rounds=1;
    
    Packets = new pair<long long,int>[traceSize];
    for(int j=0;j<rounds;j++){
        loadFile();
    }
   
}

















