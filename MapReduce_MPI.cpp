/*****************************************************************
 *   Names:                                                      *
 *   Pavan Koundinya(pkoundin,0027340763)                        *
 *   Prasanth Nunna(pnunna,0027466363)                           *
 *****************************************************************/

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
#include <mpi.h>
#include <sys/stat.h>
#include <sys/types.h>

using namespace std;
typedef map<string,int> StrIntMap;

#define NUM_FILES 80


#define NUM_READERS 4
#define NUM_MAPPERS 4
#define NUM_REDUCERS 16

#define NUM_PROCS 4
#define NUM_TOTAL_READERS (NUM_PROCS*NUM_READERS)
#define NUM_TOTAL_REDUCERS (NUM_PROCS*NUM_REDUCERS)

int totalWords[NUM_REDUCERS] = {0}, Words=0;

int main(int argc, char* argv[]){
    /********************** Variables Initialization**************************/
    double elapsed_time=0, readers=0,mappers=0,reducers=0,writers=0;
    double mpi_comm_time = 0;

    /******** MPI Variables ***************************/
    int com_size,pid;
    string outFile;

    /********Variable for Reader Threads***************/
    char files[NUM_FILES][30]={"RawText/986.txt.utf-8.txt","RawText/600.txt.utf-8.txt","RawText/39297.txt.utf-8.txt",\
    "RawText/39296.txt.utf-8.txt","RawText/39295.txt.utf-8.txt","RawText/39294.txt.utf-8.txt","RawText/39293-0.txt",\
    "RawText/39290-0.txt","RawText/39288.txt.utf-8.txt","RawText/36034.txt.utf-8.txt","RawText/34114.txt.utf-8.txt",\
    "RawText/3183.txt.utf-8.txt","RawText/27916.txt.utf-8.txt","RawText/2638.txt.utf-8.txt","RawText/2600.txt.utf-8.txt",\
    "RawText/2554.txt.utf-8.txt","RawText/1399.txt.utf-8.txt"};
    //Temporary variables and pointer for word
    string cur_word,final_word;
    int thread_num, global_thread_num;
    char* word_pointer;
    char temp_word[100];
    ifstream in[NUM_READERS];
    //queue item for each word
    StrInt cur_item;
    vector<StrInt> workq_mapper[NUM_READERS];
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
    vector<StrInt> workq_reducer[NUM_REDUCERS];
    vector<StrInt> procq_reducer[NUM_PROCS];
    //variable to hold hash value
    size_t global_queue_idx;
    StrIntMap::iterator p;
    omp_lock_t reducer_lock[NUM_REDUCERS],procq_lock[NUM_PROCS];

    /********Variables for Sender Threads**************/
    MPI_Request req[NUM_PROCS-1];
    MPI_Status status,stat[NUM_PROCS-1];
    int recv_count,index=0;

    int blocks[3]={1,1,100};
    MPI_Datatype types[3]={MPI_INT,MPI_INT,MPI_CHAR};
    MPI_Aint displacements[3];

    MPI_Datatype mpi_qitem_type;
    MPI_Aint intex,charex;


    /********Variables for Reducer Threads**************/
    //work queues for writer threads that contain final word count
    StrIntMap final_count[NUM_REDUCERS];

    /********Variables for Writer Threads**************/
    //output file
    ofstream myFile[NUM_REDUCERS];

    //Initialize MPI
    MPI_Init(&argc,&argv);
    //wait for all processors to finish MPI initialization
    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Comm_size(MPI_COMM_WORLD,&com_size);
    MPI_Comm_rank(MPI_COMM_WORLD,&pid);

    //Variables for Sender threads
    MPI_Type_extent(MPI_INT,&intex);
    MPI_Type_extent(MPI_CHAR,&charex);
    displacements[0]=static_cast<MPI_Aint>(0);
    displacements[1]=intex;
    displacements[2]=intex+intex;
    MPI_Type_create_struct(3,blocks,displacements,types,&mpi_qitem_type);
    MPI_Type_commit(&mpi_qitem_type);

    //OpenMP Initialization
    omp_set_dynamic(0);
    omp_set_num_threads(NUM_READERS+NUM_MAPPERS);
    for(int i=0;i<NUM_READERS;i++){
        omp_init_lock(&lock[i]);
    }
    for(int i=0;i<NUM_REDUCERS;i++){
        omp_init_lock(&reducer_lock[i]);
    }

    for(int i=0;i<NUM_PROCS;i++){
        omp_init_lock(&procq_lock[i]);
    }

    //if(mkdir("./MPIOutputs/",0777)==-1){
    //    cout<<"Error: Could not create directory ./MPIOutputs/"<<endl;
    //    cout<<"Delete the directory if it already exists and try again"<<endl;
    //    exit(1);
    //}

    elapsed_time-=omp_get_wtime();
    readers-=omp_get_wtime();
    mappers-=omp_get_wtime();
    /*************************************Reader Threads********************************/
    #pragma omp parallel private(thread_num,global_thread_num,cur_item,p,global_queue_idx, queue_index,cur_word,temp_word,word_pointer,final_word)
    {
        thread_num=omp_get_thread_num();
        global_thread_num = pid*NUM_READERS + thread_num;
        if(thread_num<NUM_READERS){
            for(int i=floor(global_thread_num*NUM_FILES/NUM_TOTAL_READERS);i<NUM_FILES && i<floor((global_thread_num+1)*NUM_FILES/NUM_TOTAL_READERS);i++){
                in[thread_num].open("./RawText/"+to_string((long long int)(i+1))+".txt");
                if(!in[thread_num]){
                    cout<<"File not Found: ./RawText/"<<i+1<<".txt"<<endl;
                    continue;
                }
                while(in[thread_num]>>cur_word){
                    //convert string to char* for strtok
                    strcpy(temp_word,cur_word.c_str());
                    //remove special characters at beginning and end of word
                    word_pointer=strtok(temp_word,"_|=$%&+,.:;\"\'?!-#[]()*~{}\t\r\n ");
                    if(word_pointer!=NULL){
                        final_word = string(word_pointer);
                        //convert word to all lower case letters
                        transform(final_word.begin(),final_word.end(),final_word.begin(),::tolower);
                        //create an item for the queue element
                        //cur_item.word = final_word;
                        strcpy(cur_item.word,final_word.c_str());
                        cur_item.count = 1;
                        //push the item into the queue
                        omp_set_lock(&lock[thread_num]);
                        workq_mapper[thread_num].push_back(cur_item);
                        omp_unset_lock(&lock[thread_num]);
                    }
                }
                in[thread_num].close();

                if(i==floor((global_thread_num+1)*NUM_FILES/NUM_TOTAL_READERS)-1 || i==NUM_FILES-1) {
                    omp_set_lock(&lock[thread_num]);
                    done[thread_num]=1;
                    omp_unset_lock(&lock[thread_num]);
                }
            }
            readers+=omp_get_wtime();
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
                    ++map_words[queue_index][string(workq_mapper[queue_index].back().word)];
                    workq_mapper[queue_index].erase(workq_mapper[queue_index].end()-1);
                }
                dummy_done[queue_index]=done[queue_index];
                omp_unset_lock(&lock[queue_index]);
            }

    /*************************************Sender Threads********************************/
            //take the map output and place it in the relevant reducer queue
            for(p=map_words[queue_index].begin();p!=map_words[queue_index].end();++p){
                strcpy(cur_item.word,(p->first).c_str());
                cur_item.count=p->second;
                global_queue_idx=str_hash(p->first)%NUM_TOTAL_REDUCERS;
                if(global_queue_idx < (pid+1)*NUM_REDUCERS && global_queue_idx >= pid*NUM_REDUCERS){
                    //while placing in reducer queues multiple threads might access same queue, so set lock
                    omp_set_lock(&reducer_lock[global_queue_idx-(pid*NUM_REDUCERS)]);
                    workq_reducer[global_queue_idx-(pid*NUM_REDUCERS)].push_back(cur_item);
                    omp_unset_lock(&reducer_lock[global_queue_idx-(pid*NUM_REDUCERS)]);
                } else {
                    //set the thread number
                    cur_item.thread = global_queue_idx;
                    //put it in the relevant processor queue to send
                    omp_set_lock(&procq_lock[global_queue_idx/NUM_REDUCERS]);
                    procq_reducer[global_queue_idx/NUM_REDUCERS].push_back(cur_item);
                    omp_unset_lock(&procq_lock[global_queue_idx/NUM_REDUCERS]);
                }
            }
            mappers+=omp_get_wtime();
        }
    }
    MPI_Barrier(MPI_COMM_WORLD);


    //Transmit reducer queues to all other processors
    for(int i=0;i<NUM_PROCS;i++){
        if(i!=pid){
            MPI_Issend(&(procq_reducer[i][0]),procq_reducer[i].size(),mpi_qitem_type,i,0,MPI_COMM_WORLD,&req[index]);
            index++;
        }
    }

    for(int i=0;i<NUM_PROCS-1;i++){
        //Get source of message
        MPI_Probe(MPI_ANY_SOURCE,MPI_ANY_TAG,MPI_COMM_WORLD,&status);
        //Get size of message
        MPI_Get_count(&status,mpi_qitem_type,&recv_count);
        //allocate buffer for the message
        StrInt* recv_queue = (StrInt*)malloc(sizeof(StrInt)*recv_count);
        //Receive the message
        MPI_Recv(recv_queue,recv_count,mpi_qitem_type,status.MPI_SOURCE,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
        vector<StrInt> temp_queue(recv_queue,recv_queue+recv_count);
        //Insert the words into processor queue which will be sent to reducer queue later
        procq_reducer[pid].insert(procq_reducer[pid].end(),temp_queue.begin(),temp_queue.end());
        delete recv_queue;
    }

    MPI_Waitall(NUM_PROCS-1,req,stat);

    omp_set_num_threads(NUM_REDUCERS);
    /*************************************Receiver Threads*******************************/
    #pragma omp parallel private(cur_item,global_queue_idx)
    {
        while(1){
            //Get word from the processor queue
            omp_set_lock(&procq_lock[pid]);
            if(!procq_reducer[pid].empty()){
                cur_item = procq_reducer[pid].back();
                procq_reducer[pid].erase(procq_reducer[pid].end()-1);
            } else {
                break;
            }
            omp_unset_lock(&procq_lock[pid]);

            //Check thread number and insert it in relevant reducer queue
            global_queue_idx = cur_item.thread;
            omp_set_lock(&reducer_lock[global_queue_idx-(pid*NUM_REDUCERS)]);
            workq_reducer[global_queue_idx-(pid*NUM_REDUCERS)].push_back(cur_item);
            omp_unset_lock(&reducer_lock[global_queue_idx-(pid*NUM_REDUCERS)]);
        }
        omp_unset_lock(&procq_lock[pid]);
    }

    /*************************************Reducer Threads********************************/
    #pragma omp parallel for
    for(int i=0;i<NUM_REDUCERS;i++){
        while(!workq_reducer[i].empty()){
            //combine word count in each reducer queue into final word count
            final_count[i][string(workq_reducer[i].back().word)]+=workq_reducer[i].back().count;
            workq_reducer[i].erase(workq_reducer[i].end()-1);
        }
    }

    /*************************************Writer Threads********************************/

    #pragma omp parallel private(thread_num)
    {
        thread_num = omp_get_thread_num();
        myFile[thread_num].open("WordCount_p"+to_string((long long int)pid)+"_t"+to_string((long long int)thread_num)+".txt");
        #pragma omp for private(p)
        for(int i=0;i<NUM_REDUCERS;i++){
            for(p=final_count[i].begin();p!=final_count[i].end();++p){
                totalWords[thread_num]+=p->second;
                //write count of each word into a file
                myFile[thread_num]<<p->first<<"\t"<<p->second<<"\n";
            }
        }
        myFile[thread_num].close();
    }

    for(int i=0;i<NUM_REDUCERS;i++){
        Words+=totalWords[i];
    }

    elapsed_time+=omp_get_wtime();
    cout<<"Processor "<<pid<<" Word Count = "<<Words<<endl;
    cout<<"Elapsed Time for "<<NUM_READERS<<" cores: "<<elapsed_time<<" on Processor:"<<pid<<endl;
    MPI_Finalize();

}
