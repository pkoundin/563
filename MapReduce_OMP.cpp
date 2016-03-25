#include <iostream>
#include "StrInt.h"
#include <fstream>
#include <map>
#include <string>
#include <cstring>
#include <algorithm>
#include <queue>

using namespace std;

#define NUM_FILES 1

int main(){
    /********************** Variables Initialization**************************/
    /********Variable for Reader Threads***************/
    //Array to hold file names
    char files[NUM_FILES][15]={"SomeText.txt"};
    //Temporary variables and pointer for word
    string cur_word,final_word;
    char* word_pointer;
    char temp_word[20];
    //input file stream
    ifstream in(files[0]);
    //queue item for each word
    StrInt cur_item;
    //queue for mapper threads
    queue<StrInt> workq_mapper;

    /********Variables for Mapper Threads**************/


    /********Variables for Reducer Threads**************/


    /********Variables for Writer Threads**************/



    /*************************************Reader Threads********************************/
    while(in>>cur_word){
        //convert string to char* for strtok
        strcpy(temp_word,cur_word.c_str());
        //remove special characters at beginning and end of word
        word_pointer=strtok(temp_word,",.:;\"\'?!-#[]()*");
        if(word_pointer!=NULL){
            final_word = string(word_pointer);
            //convert word to all lower case letters
            transform(final_word.begin(),final_word.end(),final_word.begin(),::tolower);
        }
        //create an item for the queue element
        cur_item.word = final_word;
        cur_item.count = 1;
        //push the item into the queue
        workq_mapper.push(cur_item);
    }

    while(!workq_mapper.empty()){
        cout<<workq_mapper.front().word<<"\t"<<workq_mapper.front().count<<endl;
        workq_mapper.pop();
    }

    /*************************************Mapper Threads********************************/


    /*************************************Reducer Threads********************************/


    /*************************************Writer Threads********************************/


}