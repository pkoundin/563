#include <iostream>
#include "StrInt.h"
#include <fstream>
#include <map>
#include <string>
#include <cstring>
#include <algorithm>
#include <queue>
#include <functional>
#include <omp.h>
#include <cmath>

using namespace std;
typedef map<string,int> StrIntMap;

#define NUM_FILES 17
//Number of Mappers and Readers are same
#define NUM_READERS 9
#define NUM_REDUCERS 2*NUM_READERS

//int totalWords = 0;

int main(){
    /********************** Variables Initialization**************************/
    double elapsed_time=0;
    /********Variable for Reader Threads***************/
    char files[NUM_FILES][30]={"RawText/986.txt.utf-8.txt","RawText/600.txt.utf-8.txt","RawText/39297.txt.utf-8.txt",\
    "RawText/39296.txt.utf-8.txt","RawText/39295.txt.utf-8.txt","RawText/39294.txt.utf-8.txt","RawText/39293-0.txt",\
    "RawText/39290-0.txt","RawText/39288.txt.utf-8.txt","RawText/36034.txt.utf-8.txt","RawText/34114.txt.utf-8.txt",\
    "RawText/3183.txt.utf-8.txt","RawText/27916.txt.utf-8.txt","RawText/2638.txt.utf-8.txt","RawText/2600.txt.utf-8.txt",\
    "RawText/2554.txt.utf-8.txt","RawText/1399.txt.utf-8.txt"};
    //Temporary variables and pointer for word
    string cur_word,final_word;
    int thread_num;
    char* word_pointer;
    char temp_word[100];
    ifstream in;
    //queue item for each word
    StrInt cur_item;
    queue<StrInt> workq_mapper[NUM_READERS];
    //flags to indicate that reader threads are done
    int done[NUM_READERS]={0},dummy_done[NUM_READERS]={0};
    omp_lock_t lock[NUM_READERS];

    /********Variables for Mapper Threads**************/
    int queue_index;
    //variable to combine words in the queue into their count
    StrIntMap map_words[NUM_READERS];
    //hash function to put words in reducer queue
    hash<string> str_hash;
    //work queue for reducer threads
    queue<StrInt> workq_reducer[NUM_REDUCERS];
    //variable to hold hash value
    size_t strhash_value;
    StrIntMap::iterator p;

    /********Variables for Reducer Threads**************/
    //work queues for writer threads that contain final word count
    StrIntMap final_count[NUM_REDUCERS];

    /********Variables for Writer Threads**************/
    //output file
    ofstream myFile;

    //Initialization
    omp_set_dynamic(0);
    omp_set_num_threads(2*NUM_READERS);
    for(int i=0;i<NUM_READERS;i++){
        omp_init_lock(&lock[i]);
    }
    elapsed_time-=omp_get_wtime();

    /*************************************Reader Threads********************************/
    #pragma omp parallel private(thread_num,cur_item,p,strhash_value, queue_index,in,cur_word,temp_word,word_pointer,final_word)
    {
        thread_num=omp_get_thread_num();

        if(thread_num<NUM_READERS){
            for(int i=floor(thread_num*NUM_FILES/NUM_READERS);i<NUM_FILES && i<floor((thread_num+1)*NUM_FILES/NUM_READERS);i++){
                in.open(files[i]);
                if(!in){
                    cout<<"File not Found:"<<files[i]<<endl;
                    continue;
                }
                while(in>>cur_word){
                    //convert string to char* for strtok
                    strcpy(temp_word,cur_word.c_str());
                    //remove special characters at beginning and end of word
                    word_pointer=strtok(temp_word,"_|=$%&+,.:;\"\'?!-#[]()*~{}\t\r\n ");
                    if(word_pointer!=NULL){
                        final_word = string(word_pointer);
                        //convert word to all lower case letters
                        transform(final_word.begin(),final_word.end(),final_word.begin(),::tolower);
                        //create an item for the queue element
                        cur_item.word = final_word;
                        cur_item.count = 1;
                        //push the item into the queue
                        omp_set_lock(&lock[thread_num]);
                        workq_mapper[thread_num].push(cur_item);
                        omp_unset_lock(&lock[thread_num]);
                    }
                }
                in.close();

                if(i==floor((thread_num+1)*NUM_FILES/NUM_READERS)-1 || i==NUM_FILES-1) {
                    omp_set_lock(&lock[thread_num]);
                    done[thread_num]=1;
                    omp_unset_lock(&lock[thread_num]);
                }
            }

        }

    /*************************************Mapper Threads********************************/
        else{
            queue_index=thread_num-NUM_READERS;
            omp_set_lock(&lock[queue_index]);
            dummy_done[queue_index]=done[queue_index];
            omp_unset_lock(&lock[queue_index]);
            while(!dummy_done[queue_index]){
                omp_set_lock(&lock[queue_index]);
                //take items from the queue and determine word count in the queue
                while(!workq_mapper[queue_index].empty()){
                    ++map_words[queue_index][workq_mapper[queue_index].front().word];
                    workq_mapper[queue_index].pop();
                }
                dummy_done[queue_index]=done[queue_index];
                omp_unset_lock(&lock[queue_index]);
            }

            //take the map output and place it in the relevant reducer queue
            for(p=map_words[queue_index].begin();p!=map_words[queue_index].end();++p){
                cur_item.word=p->first;
                cur_item.count=p->second;
                strhash_value=str_hash(p->first)%NUM_REDUCERS;
                //while placing in reducer queues multiple threads might access same queue
                //place it in critical section
                #pragma omp critical
                workq_reducer[strhash_value].push(cur_item);
            }
        }
    }

    omp_set_num_threads(NUM_REDUCERS);

    /*************************************Reducer Threads********************************/
    #pragma omp parallel for
    for(int i=0;i<NUM_REDUCERS;i++){
        while(!workq_reducer[i].empty()){
            //combine word count in each reducer queue into final word count
            final_count[i][workq_reducer[i].front().word]+=workq_reducer[i].front().count;
            workq_reducer[i].pop();
        }
    }

    /*************************************Writer Threads********************************/
    myFile.open("omp_output.txt");
    #pragma omp parallel for private(p)
    for(int i=0;i<NUM_REDUCERS;i++){
        for(p=final_count[i].begin();p!=final_count[i].end();++p){
            #pragma omp critical
            {
                //totalWords+=p->second;
                //write count of each word into a file
                myFile<<p->first<<"\t"<<p->second<<"\n";
            }
        }
    }
    myFile.close();
    elapsed_time+=omp_get_wtime();

    cout<<"Elapsed Time for "<<NUM_READERS<<" cores: "<<elapsed_time<<endl;

}