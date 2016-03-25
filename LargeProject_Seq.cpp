#include <iostream>
#include <fstream>
#include <map>
#include <string>
#include <cstring>
#include <algorithm>

using namespace std;

typedef map<string, int> StrIntMap;

void countWords(istream& in, StrIntMap& words) 
{
string s, word_string; 
char* final_word; 

char word[100];

	while (in >> s) 
	{
		strcpy(word,s.c_str());
		final_word=strtok(word,",.:;\"\'?!-#[]()*");
		if(final_word!=NULL){
			word_string=string(final_word);	
			transform(word_string.begin(), word_string.end(), word_string.begin(), ::tolower);
			++words[word_string];
		}
	}
}

int main(int argc, char** argv) 
{
	if (argc < 2)
		return(-1);
	ifstream in(argv[1]);   
	if (!in)
		return(-1);
	StrIntMap w;
	countWords(in, w);
	for (StrIntMap::iterator p = w.begin();p != w.end(); ++p) 
	{
	        cout << p->first << " occurred " << p->second << " times.\n";
	}
}
