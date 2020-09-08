#ifndef _RECORDMANAGER_H_
#define _RECORDMANAGER_H_

// (C) 2020, Yuxuan Zhou
// RecordManager.h
// All rights reserved.


#include <cstdio> 
#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include"CatalogManager.h"
#include"BufferManager.h"

using namespace std;

bool insert_record(string str);

void delete_record(string str);

void select_record(string str);

bool isUnique(vector<string> tuple);
void print_AttrName(string table_name);
void print_tuple(char* datap, string table_name);

void delete_tuple(char* datap, string table_name, RecordBlock* de_record);

bool isWhereOne(char* datap, vector<string> input, vector<int> Attr_Lengths, vector<string> Attr_Name, vector<string>Attr_Type);

bool isWhereTwo(char* datap, vector<string> input, vector<int> Attr_Lengths, vector<string> Attr_Name, vector<string>Attr_Type);

bool judge(int* value, vector<string> input);
bool judge(float* value, vector<string> input);
bool judge(char* value, vector<string> input);
#endif