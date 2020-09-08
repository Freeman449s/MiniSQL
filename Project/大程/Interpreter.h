#pragma once
#include"RecordManager.h"
#include <iostream>
#include <string>
#include <fstream>
using namespace std;

#define ERROR_WRONG_INPUT     "错误输入！"
#define ERROR_USELESS_INPUT   "无效的命令！"
#define ERROR_FILE_NOT_EXIST "无法打开文件！"
#define ERROR_NOTFULL_CMD "错误的文件命令！"
#define ERROR_USELESS_CREATE "错误的create命令！"
#define ERROR_USELESS_DELETE "错误的delete命令！"
#define ERROR_USELESS_DROP "错误的drop命令！"
#define ERROR_USELESS_INSERT "错误的insert命令！"
#define ERROR_USELESS_SELECT "错误的select命令！"
#define ERROR_TABLE_NOT_EXIST "此表不存在！"
#define ERROR_INDEX_NOT_EXIST "此索引不存在！"
#define ERROR_MAKING_CREATE_TABLE "此表无法创建！"
#define ERROR_MAKING_CREATE_INDEX "此索引无法创建！"
#define ERROE_CREATE_INDEX "无效的创建索引命令！"
#define ERROE_CREATE_TABLE "无效的创建表命令！"
#define ERROR_INSERT_WRONG "无效的插入命令！"





#define CREATE_TABLE         1
#define CREATE_INDEX         2
#define CREATE_DATABASE      3
#define DELETE_TABLE         4
#define DROP_TABLE           5
#define DROP_INDEX           6
#define DROP_DATABASE        7


string error_info;					//记录错误信息
string translated_record;           //被翻译后的记录
int translated_record_type;         //记录的种类
string check_delete_type(string input);
void use_interpreter();
bool get_input(string & user_input);
bool check_type(string user_input);
bool string_to_int(string int_string, int & result);
bool check_unique(string & string_input);
bool check_create(string user_input, int &attr_num);
string check_drop(string user_input);
string check_insert(string user_input);
string check_create_table(string user_input, int &attr_num);
string check_create_index(string user_input);
//string check_create_database(string user_input);//database不进行实现
string check_delete(string user_input);
string check_select(string user_input);
bool open_execfile(string & file_input);
void output_select_ans();



